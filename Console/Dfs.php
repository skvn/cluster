<?php

namespace Skvn\Cluster\Console;

use Skvn\App\Events\ConsoleActionEvent;
use Skvn\Base\Traits\SelfDescribe;
use Skvn\Cluster\Cluster;
use Skvn\Cluster\Exceptions\ClusterException;

/**
 * Dfs operations
 * @package Skvn\App\Console
 */
class Dfs extends ConsoleActionEvent
{
    use SelfDescribe;

    /**
     * Update files from queue
     */
    function actionUpdate()
    {
        $queue = $this->app->cluster->fetchQueue();
        $master = $this->app->cluster->getMasterHost();
        $last_id = 0;
        foreach ($queue ?? [] as $file) {
            if ($file['action'] == Cluster :: FILE_DELETE) {
                $filename = $this->app->getPath('@root/' . $file['filename']);
                if (file_exists($filename)) {
                    unlink($filename);
                    $this->app->cluster->log('DELETED', $file['filename']);
                } else {
                    $this->app->cluster->log('DELETE FAILED', $file['filename']);
                }
            }
            if ($file['action'] == Cluster :: FILE_PUSH) {
                $filename = $this->app->getPath('@root/' . $file['filename']);
                if ($master === false) {
                    throw new ClusterException('Master host is not set');
                }
                $fetched = $this->app->cluster->fetchFileFromHost($file['filename'], $master);
                if ($fetched) {
                    if (md5_file($filename) != $file['checksum']) {
                        $this->app->cluster->log('CHECKSUM ERROR', $file['filename']);
                    }
                }
            }
            $last_id = $file['id'];
        }
        $this->app->cluster->callHost($master, 'update_client', ['last_event' => $last_id, 'last_event_time' => time()]);
        $this->app->cluster->log('UPDATED', count($queue) . ' executed');
    }


}