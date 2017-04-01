<?php

namespace Skvn\Cluster\Console;

use Skvn\App\Events\ConsoleActionEvent;
use Skvn\Base\Traits\SelfDescribe;
use Skvn\Cluster\Cluster;
use Skvn\Cluster\Exceptions\ClusterException;
use Skvn\Event\Events\Log;

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
        $params = ['last_event_time' => time()];
        if ($last_id > 0) {
            $params['last_event'] = $last_id;
        }
        $this->app->cluster->callHost($master, 'update_client', $params);
        $this->app->cluster->log('UPDATED', count($queue) . ' executed');
    }

    /**
     * Update heartbeat marker
     */
    function actionHeartbeat()
    {
        $this->app->cluster->heartbeat();
    }

    /**
     * Check replication state on cluster node
     */
    function actionCheckHeartbeat()
    {
        if (!$this->app->cluster->checkHeartbeat()) {
            $this->stdout('Replication broken');
            $this->app->triggerEvent(new \Skvn\Event\Events\NotifyProblem([
                'problem' => 'cluster_heartbeat',
                'host_id' => $this->app->cluster->getOption('my_id')
            ]));
        }
    }

    /**
     * Ping all nodes
     */
    function actionPing()
    {
        $down = $this->app->cluster->pingNodes();
        if (count($down) > 0) {
            foreach ($down as $host_id) {
                $this->app->triggerEvent(new \Skvn\Event\Events\NotifyProblem([
                    'problem' => 'cluster_host_down',
                    'host_id' => $host_id
                ]));
            }
        }
    }


    /**
     * Scheduled rsync data sync
     */
    function actionSync()
    {
        $startTime = time();
        $schedule = $this->app->cluster->getOption('sync_schedule');
        $action = $schedule[date('H:i')] ?? null;
        if (empty($action)) {
            $this->app->triggerEvent(new Log(['message' => "No time", 'category' => 'dfs_sync']));
            return;
        }
        if ($action['source'] !== intval($this->app->cluster->getOption('my_id'))) {
            $this->app->triggerEvent(new Log(['message' => "Foreign source", 'category' => 'dfs_sync']));
            return;
        }
        if (!empty($action['section'])) {
            $this->app->triggerEvent(new Log(['message' => "Syncing", 'category' => 'dfs_sync', 'info' => $action]));
            $t = microtime(true);
            $result = $this->syncSection($action['section'], $action['target']);
            if (!empty($result)) {
                $this->app->triggerEvent(new Log([
                    'message' => "Synced",
                    'category' => 'dfs_sync',
                    'result' => $result,
                    'startTime' => $startTime,
                    'info' => $action,
                    'time' => round(microtime(true) - $t, 1)
                ]));
                $subject = 'DSF SYNC: ' . json_encode($action);
                $this->app->triggerEvent(new \Skvn\Event\Events\NotifyRegular(['subject' => $subject, 'message' => implode(PHP_EOL, $result)]));
                return;
            }
            $this->app->triggerEvent(new \Skvn\Event\Events\Log(['message' => "Unknown result", 'category' => 'dfs_sync']));
            return;
        }
        $this->app->triggerEvent(new \Skvn\Event\Events\Log(['message' => "No section", 'category' => 'dfs_sync']));
    }


    private function syncSection($section, $target)
    {
        $t = microtime(true);
        $this->app->db->disconnect();
        $command = $this->app->cluster->getOption('rsync_command') . ' ';
        $command .= $this->app->getPath($this->app->cluster->getOption("sections_path")) . "/" . $section . "/ ";
        $host = $this->app->cluster->getHostById($target);
        $command .= $host['img'] . "::section" . $section;
        exec($command, $result);
        $strings = [];
        $strings[] = "Section " . $section . " copied to node " . $target;
        $strings[] = "Result: ";
        $strings = array_merge($strings, $result);
        $strings[] = '';
        $strings[] = 'Process done in ' . round(microtime(true)-$t, 1) . ' seconds';
        return $strings;
    }


}