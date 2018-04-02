<?php

namespace Skvn\Cluster;

use Skvn\Base\Traits\AppHolder;
use Skvn\Base\Helpers\Str;
use Skvn\Event\Events\Log as LogEvent;
use Skvn\Event\Event as BaseEvent;
use Skvn\Base\Helpers\File;


class Cluster
{
    use AppHolder;

    protected $config;
    protected $hosts = [];
    protected $downNodes = null;
    protected $imgcacheHandlers = [];

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
    
    function getClusterHosts($withSelf = false)
    {
        $hosts = $this->hosts;
        if ($withSelf) {
            $hosts[$this->config['my_id']] = $this->getHostByid($this->config['my_id']);
        }
        return $hosts;
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

    function isMaster()
    {
        return $this->getMasterHost() === false;
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
        $result = $this->app->db->update($this->config['client_table'], $data);
        if (!empty($data['last_event'])) {
            $row = $this->app->db->selectOne('select min(last_event) as min_event from ' . $this->config['client_table']);
            if (!empty($row['min_event'])) {
                $this->app->db->statement('delete from ' . $this->config['queue_table'] . ' where id < ?', [intval($row['min_event'])]);
                if (date('N') == 7 && date('Hi') > 23000) {
                    $this->app->db->statement('optimize table ' . $this->config['queue_table']);
                }
                $this->log('FLUSH QUEUE', $row['min_event']);
            }
        }
        return $result;
    }

    function clear()
    {
        if ($this->config['is_master'])
        {
            $row = \DB :: selectOne("select min(last_event) as min_event from `".$this->config['client_table']."`");
            $min_event = isset($row['min_event']) ? $row['min_event'] : 0;
            \DB :: statement("delete from `".$this->config['queue_table']."` where id<".intval($min_event));
            $this->log("QUEUE: flushed at " . $min_event. " event");
            return array('status' => "done", "message" => "Queue cleaned");
        }
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


    function triggerEvent(BaseEvent $event)
    {
        $this->app->triggerEvent($event);
        $args = $event->payload();
        $args['event'] = get_class($event);
        foreach ($this->hosts as $host) {
            $this->callHost($host, 'triggerEvent', $args);
        }
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

    function getDeployFilename($service = null, $filename = null)
    {
        $path = $this->app->getPath('@var/depl');
        if ($service) {
            $path .= '/' . $service;
            if ($filename) {
                $path .= $filename;
            }
        }
        return $path;
    }

    function startDeployService($service)
    {
        $conf = $this->config['deploy'][$service];
        if (empty($conf)) {
            throw new Exceptions\ClusterException('Service ' . $service . ' not found');
        }
        $files = [];
        if (!empty($conf['files'])) {
            foreach ($conf['files'] as $file) {
                $files[$file] = base64_encode(file_get_contents($file));
            }
        }
        foreach ($this->hosts as $host) {
            $this->callHost($host, 'storeDeployData', [
                'service' => $service,
                'files' => $files
            ]);
        }
    }

    function getImgcacheHandler($handler, $args = null)
    {
        if (!isset($this->imgcacheHandlers[$handler])) {
            $class = $this->config['imgcache']['handlers'][$handler] ?? null;
            if (empty($class)) {
                throw new Exceptions\ImgcacheException('Unknown imgcache handler: ' . $handler, [], -601);
            }
            if (!class_exists($class)) {
                throw new Exceptions\ImgcacheException('Imgcache handler ' . $handler . ' does not exist', [], -601);
            }
            $this->imgcacheHandlers[$handler] = new $class();
            $this->imgcacheHandlers[$handler]->path = $this->app->getPath($this->config['imgcache']['path'] . '/' . $handler);
            if (!empty($this->config['imgcache']['img_worker'])) {
                $this->imgcacheHandlers[$handler]->imgWorker = $this->config['imgcache']['img_worker'];
            }
        }
        if (!is_null($args)) {
            if (!$this->imgcacheHandlers[$handler]->validate($args)) {
                throw new Exceptions\ImgcacheException('Invalid arguments for ' . $handler . ' imgcache handler: ' . json_encode($args) . PHP_EOL . $this->imgcacheHandlers[$handler]->usage());
            }
        }
        return $this->imgcacheHandlers[$handler];
    }


    function getImgcachePath($handler, $args, $forceCache = false)
    {
        $hObj = $this->getImgcacheHandler($handler, $args);
        $target = $hObj->getTargetPathByArgs($args);
        if ($forceCache) {
            $this->getImgcacheTarget($handler, $args, $this->app->getPath($this->config['imgcache']['path'] . '/' . $handler . '/' . $target));
        }
        return $this->app->getPath($this->config['imgcache']['path'] . '/' . $handler . '/' . $target);
    }

    function getImgcacheUrl($handler, $args, $forceCache = false)
    {
        $hObj = $this->getImgcacheHandler($handler);
        $target = $hObj->getTargetPathByArgs($args);
        if ($forceCache) {
            $this->getImgcacheTarget($handler, $args, $this->app->getPath($this->config['imgcache']['path'] . '/' . $handler . '/' . $target));
        }
        if ($hObj->distributed && $hObj->getDistributedKey($args) > 0) {
            $host = $this->getHost(null, $hObj->getDistributedKey($args), ['ts' => $hObj->getLastChanged($args)]);
        } else {
            $host = $this->getHostById($this->config['my_id'])['img'];
        }
        return $this->app->config('app.protocol') . $host . $this->config['imgcache']['url'] . '/' . $handler . '/' . $target;
    }

    function parseImgcacheUrl($source, $forceCache = true)
    {
        $url = $source;
        if (Str :: pos('http', $url) === 0) {
            $url = parse_url($url, PHP_URL_PATH);
        }
        if (Str :: pos('?', $url) !== false) {
            $url = preg_replace('#\?.*$#', '', $url);
        }
        if (empty($url)) {
            throw new Exceptions\ImgcacheException($source . ' url is invalid', [], -602);
        }
        $url = substr($url, strlen($this->config['imgcache']['url'])+1);
        $segments = explode('/', $url, 2);
        if (count($segments) != 2) {
            throw new Exceptions\ImgcacheException($source . ' url is invalid', [], -602);
        }
        $hObj = $this->getImgcacheHandler($segments[0]);
        $args = $hObj->getArgsByTargetPath($segments[1]);
        if ($forceCache) {
            $args['imgObj'] = $this->getImgcacheTarget($segments[0], $args, $args['target'] = $this->app->getPath($this->config['imgcache']['path'] . '/' . $segments[0] . '/' . $segments[1]));
        }
        return $args;
    }

    function getImgcacheTarget($handler, $args, $target = null)
    {
        if (!empty($args['noregenerate']) && !empty($target)) {
            if (File :: safeExists($target) && filesize($target) > 0) {
                $class = $this->config['imgcache']['img_worker'];
                return new $class($target);
            }
        }
        $hObj = $this->getImgcacheHandler($handler);
        $img = $hObj->buildTargetImage($args);
        if (!empty($args['size'])) {
            list($w, $h) = explode('x', $args['size']);
            $img->smartResize($w, $h);
        }
        if (!empty($target)) {
            File :: safeMkdir(dirname($target));
            $img->writeImage($target);
        }
        return $img;
    }

    function deleteImgCache($handler, $args)
    {
        $hObj = $this->getImgcacheHandler($handler);
        $hObj->removeTarget($args);
    }

    function processDeployService($service)
    {
        $result = [];
        $conf = $this->config['deploy'][$service];
        if (empty($conf || empty($conf['restartcmd']))) {
            throw new Exceptions\ClusterException('Service ' . $service . ' not found');
        }
        foreach ($conf['files'] ?? [] as $file) {
            if (!file_exists($this->getDeployFilename($service, $file))) {
                throw new Exceptions\ClusterException('Deploy failed: ' . $file . ' not found');
            }
        }
        foreach ($conf['files'] ?? [] as $file) {
            if (file_exists($file)) {
                unlink($file);
            }
            copy($this->getDeployFilename($service, $file), $file);
            $result[] = $file . ' updated';
        }
        exec($conf['restartcmd'], $output);
        $result[] = '<bold>' . $conf['restartcmd'] . '</bold> executed';
        $result = array_merge($result, $output);
        $this->app->triggerEvent(new \Skvn\Event\Events\NotifyProblem([
            'problem' => 'deploy-done',
            'service' => $service,
            'host_id' => $this->config['my_id']
        ]));
        return $result;
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