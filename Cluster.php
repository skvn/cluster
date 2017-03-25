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
        $file = $this->normalizeFile($file);
        if (file_exists($this->app->getPath('@root/' . $file))) {
            return $file;
        }
        $host = $this->getMasterHost();
        if ($host) {
            return $this->fetchFileFromHost($file, $host);
        }
        return false;
    }

    function get($f)
    {

        if (!empty($this->config['is_slave']))
        {
            $result  = mtoSoapService :: callService($this->config['master_wsdl'], "getFile", array(
                'login' => $this->config['login'],
                'password' => $this->config['password'],
                'filename' => $this->_fn($f)
            ));
            if (!isset($result['content']))
            {
                $this->log("NOT FOUND: " . $f);
                return false;
            }
            $filename = $this->root . '/' . $this->_fn($f);
            if (file_exists($filename))
            {
                unlink($filename);
            }
            mtoFs :: mkdir(dirname($filename));
            file_put_contents($filename, base64_decode($result['content']));
            $this->log("GET: " . $f);
            return $f;
        }
        else
        {
            $this->log("NOT FOUND: " . $f);
            return false;
        }
    }


    function pushFileSync($file)
    {
        $file = $this->normalizeFilename($file);
        $this->queueFile($file, static :: FILE_PUSH);
        foreach ($this->hosts as $host_id => $host) {
            $this->log('CALL PUSH', $file, ['host' => $host['ctl']]);
            $this->app->api->call($host['ctl'], 'dfs/fetch', ['file' => $file, 'host_id' => $this->config['my_id']]);
        }
    }

    function deleteFileSync($file)
    {
        $file = $this->normalizeFilename($file);
        $this->queueFile($file, static :: FILE_DELETE);
        foreach ($this->hosts as $host_id => $host) {
            $this->log('CALL DELETE', $file, ['host' => $host['ctl']]);
            $this->app->api->call($host['ctl'], 'dfs/delete', ['file' => $file, 'host_id' => $this->config['my_id']]);
        }
    }

    function getHostById($host_id)
    {
        if (!isset($this->config['hosts'][$host_id])) {
            throw new Exceptions\ClusterException('Host ' . $host_id . ' does not exist');
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

    function init()
    {
        $sql = [];
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