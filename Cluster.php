<?php

namespace Skvn\Cluster;

use Skvn\Base\Traits\AppHolder;
use Skvn\Base\Helpers\Str;
use Skvn\Event\Events\Log as LogEvent;


class Cluster
{
    use AppHolder;

    protected $config;
    protected $hosts = [];
    protected $downNodes = null;

    const FILE_PUSH = 2;
    const FILE_DELETE = 3;

    function __construct($config)
    {
        $this->config = $config;
        $siblings  = !empty($this->config['siblings']) ? explode(',', $this->config['siblings']) : [];
        foreach ($siblings as $host_id) {
            if (isset($this->config['hosts'][$host_id])) {
                $this->hosts[$host_id] = $this->config['hosts'][$host_id];
            }
        }
    }

    function getConfig()
    {
        return $this->config;
    }

    function getOption($name)
    {
        return $this->config[$name] ?? null;
    }

    function getAllHosts()
    {
        return $this->config['hosts'];
    }

    function pushFile($file)
    {
        $file = $this->normalizeFilename($file);
        if (file_exists($file) && is_file($file)) {
            foreach ($this->config['exclude_paths'] as $path) {
                if (Str :: pos($path, $file) !== false) {
                    return;
                }
            }
            $this->log('QUEUE PUSH', $file);
            $this->app->triggerEvent(new Events\PushFile(['file' => $file]));
        }
    }

    function deleteFile($file)
    {
        $file = $this->normalizeFilename($file);
        if (file_exists($file) && is_file($file)) {
            foreach ($this->config['exclude_paths'] as $path) {
                if (Str :: pos($path, $file) !== false) {
                    return;
                }
            }
            $this->log('QUEUE DELETE', $file);
            $this->app->triggerEvent(new Events\DeleteFile(['file' => $file]));
        }
        if (file_exists($this->app->getPath('@root/' . $file))) {
            unlink($this->app->getPath('@root/' . $file));
        }

    }

    function fetchFileFromHost($file, $host)
    {
        $filename = $this->app->getPath('@root/' . $file);
        if (!file_exists(dirname($filename))) {
            mkdir(dirname($filename), 0777, true);
        }
        $url = $this->getFileUrl($file, $host['img']);
        $fp = fopen($filename, 'w');
        try {
            $this->app->urlLoader->load($url, [], ['returntransfer' => false, 'file' => $fp]);
            $this->log('FETCHED', $file);
            $result = true;
        }
        catch (\Exception $e) {
            $this->log('FETCH FAILED', $file, ['error' => $e->getMessage()]);
            unlink($filename);
            $result = false;
        }
        fclose($fp);
        return $result;
    }

    function getFile($file)
    {
        $file = $this->normalizeFilename($file);
        if (file_exists($this->app->getPath('@root/' . $file))) {
            return $file;
        }
        $host = $this->getMasterHost();
        if ($host) {
            return $this->fetchFileFromHost($file, $host);
        }
        return false;
    }

    function callHost($host, $method, $args = [])
    {
        $args['host_id'] = $this->config['my_id'];
        return $this->app->api->call($host['ctl'], 'dfs/' . $method, $args);
    }

    function pushFileSync($file)
    {
        $file = $this->normalizeFilename($file);
        $this->queueFile($file, static :: FILE_PUSH);
        foreach ($this->hosts as $host_id => $host) {
            $this->log('CALL PUSH', $file, ['host' => $host['ctl']]);
            $this->callHost($host, 'fetch', ['file' => $file]);
        }
    }

    function deleteFileSync($file)
    {
        $file = $this->normalizeFilename($file);
        $this->queueFile($file, static :: FILE_DELETE);
        foreach ($this->hosts as $host_id => $host) {
            $this->log('CALL DELETE', $file, ['host' => $host['ctl']]);
            $this->callHost($host, 'delete', ['file' => $file]);
        }
    }

    function getHostById($host_id, $fullInfo = false)
    {
        if (!isset($this->config['hosts'][$host_id])) {
            throw new Exceptions\ClusterException('Host ' . $host_id . ' does not exist');
        }
        if ($fullInfo) {
            $this->loadHostInfo($host_id);
        }
        return $this->config['hosts'][$host_id];
    }

    function getMasterHost()
    {
        if (!empty($this->config['master_host'])) {
            return $this->getHostById($this->config['master_host']);
        }
        return false;
    }

    function getFileUrl($file, $host = null)
    {
        $file = $this->normalizeFilename($file);
        foreach ($this->config['http_map'] as $path => $url) {
            if (Str :: pos($path, $file) === 0) {
                $target = preg_replace('#^' . $path . '#', $url, $file);
                if (!is_null($host)) {
                    $target = $this->app->config('app.protocol') . $host . $target;
                }
                return $target;
            }
        }
        throw new Exceptions\ClusterException('File ' . $file . ' is not available for http access');
    }

    function fetchQueue()
    {
        $host = $this->getMasterHost();
        if ($host === false) {
            return;
        }
        $queue = $this->callHost($host, 'queue');
        $this->log('QUEUE RECEIVED', count($queue) . ' events');
        return $queue;
    }

    function getQueueForHost($host_id)
    {
        $host = $this->getHostById($host_id, true);
        $queue = $this->app->db->select('select * from ' . $this->config['queue_table'] . ' where id > ? order by id limit 500', [$host['last_event']]);
        $this->log('QUEUE FETCH', $host_id, ['count' => count($queue)]);
        return $queue;
    }

    private function queueFile($file, $action)
    {
        $hash = $action == static :: FILE_DELETE ? '' : md5_file($file);
        $this->app->db->insert($this->config['queue_table'], [
            'filename' => $file,
            'action' => $action,
            'checksum' => $hash,
            'host_id' => $this->config['my_id'],
            'event_time' => time()
        ]);
    }

    private function loadHostInfo($host_id)
    {
        if (!isset($this->config['hosts'][$host_id])) {
            throw new Exceptions\ClusterException('Host ' . $host_id . ' does not exist');
        }
        if (!empty($this->config['hosts'][$host_id]['id'])) {
            return;
        }
        $row = $this->app->db->selectOne('select * from ' . $this->config['client_table'] . ' where id=?', [$host_id]);
        if (empty($row)) {
            $row = [
                'id' => $host_id,
                'last_event' => null,
                'last_sync' => 0,
                'last_event_time' => null,
                'last_pinged' => 0
            ];
            $this->app->db->insert($this->config['client_table'], $row);
        }
        foreach ($row as $k => $v) {
            $this->config['hosts'][$host_id][$k] = $v;
            if (isset($this->hosts[$host_id])) {
                $this->hosts[$host_id][$k] = $v;
            }
        }
    }

    private function normalizeFilename($filename)
    {
        if (Str :: pos('/', $filename) === 0) {
            if (Str :: pos($this->app->getPath('@root'), $filename) !== 0) {
                throw new Exceptions\ClusterException('Filename ' . $filename . ' is not under the storage root');
            }
            $filename = str_replace($this->app->getPath('@root/'), '', $filename);
        }
        return $filename;
    }

    function log($op, $target, $info = [])
    {
        if (!empty($this->config['logging'])) {
            $args = [
                'message' => $op . ': ' . $target,
                'category' => 'dfs'
            ];
            if (!empty($info)) {
                $args['message'] .= ' ::: ' . json_encode($info);
            }
            $this->app->triggerEvent(new LogEvent($args));
        }
    }

    function updateClient($data)
    {
        return $this->app->db->update($this->config['client_table'], $data);
    }

    function isSectionLocked($section = null, $skey = null)
    {
        if (is_null($section)) {
            $section = $this->getSection($skey);
        }
        return in_array($section, $this->config['locked_sections']);
    }

    function getSection($skey)
    {
        foreach ($this->config['shards'] as $k => $section) {
            if ($skey >= $section[0] && $skey <= $section[1]) {
                return $k;
            }
        }
        throw new Exceptions\ClusterException('Section not found for key ' . $skey);
    }

    function getHost($section = null, $skey = null, $args = [], $param = 'img')
    {
        if (is_null($section)) {
            $section = $this->getSection($skey);
        }
        $hosts = explode(',', $this->config['shard_distr'][$section] ?? '');
        if (empty($hosts)) {
            throw new Exceptions\ClusterException('Hosts not found for section ' . $section);
        }
        shuffle($hosts);
        $host_id = $hosts[0];
        if (!empty($args['ts']) && $args['ts'] > (time() - $this->config['ping_threshold']*60)) {
            $host_id = $this->config['my_id'];
        }
        if ($this->isHostDown($host_id)) {
            $host = $this->getMasterHost();
        } else {
            $host = $this->getHostById($host_id);
        }
        return !empty($param) ? $host[$param] : $host;

    }

    function isHostDown($host_id)
    {
        if (is_null($this->downNodes)) {
            $marker = $this->app->getPath($this->config['down_host_marker']);
            if (file_exists($marker)) {
                $this->downNodes = array_keys(parse_ini_file($marker));
            } else {
                $this->downNodes = [];
            }
        }
        return in_array($host_id, $this->downNodes);
    }





    function heartbeat()
    {
        $this->app->db->statement('update ' . $this->config['heartbeat_table'] . ' set ' . $this->config['heartbeat_field'] . '=' . time());
    }

    function checkHeartbeat()
    {
        $slave = $this->app->db->selectOne("show slave status");
        $hb = $this->app->db->selectOne("select * from " . $this->config['heartbeat_table']);
        $io = $slave['Slave_IO_Running'] ?? 'No';
        $sql = $slave['Slave_SQL_Running'] ?? 'No';
        $hbts = $hb[$this->config['heartbeat_field']] ?? 0;
        if ($io != "Yes" || $sql != "Yes" || (time()-$hbts) > ($this->config['heartbeat_warning'] * 60))
        {
            return false;
        }
        return true;
    }

    function pingNodes()
    {
        $down = [];
        foreach ($this->hosts as $host_id => $host) {
            $result = $this->callHost($host, 'ping');
            if ($result === true) {
                $this->updateClient(['id' => $host_id, 'last_pinged' => time()]);
            }
        }
        $failed = $this->app->db->select("select * from ". $this->config['client_table']." where last_pinged < ?", [time() - $this->config['ping_threshold'] * 60]);
        if ($failed) {
            $marker = $this->app->getPath($this->config['down_host_marker']);
            if (file_exists($marker)) {
                $list = parse_ini_file($marker);
            } else {
                $list = [];
            }
            foreach ($failed as $host) {
                if (!array_key_exists($host['id'], $list)) {
                    $list[$host['id']] = 1;
                    $down[] = $host['id'];
                }
            }
            $strings = [];
            foreach ($list as $k => $v) {
                $strings[] = $k . ' = ' . $v;
            }
            file_put_contents($marker, implode(PHP_EOL, $strings));

        }
        return $down;
    }


    function init()
    {
        $sql = [];
        $sql[] = "drop table if exists `".$this->config['client_table']."`";
        $sql[] = "CREATE TABLE `".$this->config['client_table']."` (
              `id` int(11) NOT NULL,
              `last_event` int(11) DEFAULT NULL,
              `last_sync` int(11) DEFAULT '0',
              `last_event_time` int(11) DEFAULT NULL,
              `last_pinged` int(11) DEFAULT '0',
              PRIMARY KEY (`id`)
            ) engine=innodb
        ";
        $sql[] = "CREATE TABLE `".$this->config['queue_table']."` (
             `id` int(11) NOT NULL AUTO_INCREMENT,
             `host_id` int(11) default 0,
             `filename` varchar(255) DEFAULT NULL,
             `event_time` int(11) default null,
             `action` int(11) DEFAULT NULL,
             `checksum` varchar(32) default NULL,
             PRIMARY KEY (`id`)
           ) engine=innodb";
        foreach ($sql as $query)
        {
            $this->app->db->statement($query);
        }

        return ['status' => "done", 'message' => "Tables created"];
    }

}