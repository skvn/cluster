<?php

namespace Skvn\Cluster;

use Skvn\Base\Traits\AppHolder;
use Skvn\Base\Helpers\Str;

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
        foreach ($siblings as $host_id => $host) {
            if (isset($this->config['hosts'][$host_id])) {
                $this->hosts[$host_id] = $host;
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
            $this->app->triggerEvent(new Events\PushFile(['file' => $file]));
        }
    }

    function pushFileSync($file)
    {
        $file = $this->normalizeFilename($file);
        $this->queueFile($file, static :: FILE_PUSH);
        foreach ($this->hosts as $host_id => $host) {
            $this->app->api->call($host['ctl'], 'dfs/fetch', ['file' => $file]);
        }
    }

    function getHostById($host_id)
    {
        if (!isset($this->config['hosts'][$host_id])) {
            throw new Exceptions\ClusterException('Host ' . $host_id . ' does not exist');
        }
        return $this->config['hosts'][$host_id];
    }

    private function queueFile($file, $action)
    {
        $hash = $action == static :: FILE_DELETE ? '' : md5_file($file);
        $this->db->insert($this->config['queue_table'], [
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




    function put($f, $queued = false)
    {
                    if ($queued)
                    {
                        //mtoQueue :: create("dfs")->createEvent("dfs_push", array('file' => $f));
                        //\App :: triggerEvent(new \App\Events\DfsPush(['file' => $f]));
                    }
                    else
                    {
                        //$this->createEvent($this->_fn($f), self :: ACTION_REPLACE);
                        foreach ($this->cdn->getAllHosts() as $host_id => $host)
                        {
                            if ($host_id != $this->cdn->getOption("my_id") && !in_array($host_id, [1, 2, 99]))
                            {
                                mtoApi :: callJson("/dfs/fetch", array('file' => $this->_fn($f), 'async' => 1), array('host' => $host['ctl']));
                            }
                        }
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