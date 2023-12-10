<?php

namespace Skvn\Cluster\Console;

use Skvn\App\Events\ConsoleActionEvent;
use Skvn\Base\Traits\SelfDescribe;
use Skvn\Cluster\Cluster;
use Skvn\Cluster\Exceptions\ClusterException;
use Skvn\Event\Events\Log;
use Skvn\Cluster\Events\ValidateSection;
use Skvn\Event\Contracts\ScheduledEvent;
use Skvn\Event\Traits\Scheduled;
use Skvn\Base\Helpers\File;

/**
 * Dfs operations
 * @package Skvn\App\Console
 */
class Dfs extends ConsoleActionEvent implements ScheduledEvent
{
    use SelfDescribe;
    use Scheduled;


    function schedule()
    {
        $schedule = [];
        foreach ($this->app->cluster->getOption('schedule') as $entry) {
            $entry['time'] = $this->scheduleByString($entry['time']);
            $schedule[] = $entry;
        }
        foreach ($this->app->cluster->getOption('sync_schedule') as $t => $entry) {
            $action = null;
            $opts = [];
            if (!empty($entry['section'])) {
                $action = 'sync-section';
                $opts = ['target' => $entry['target'], 'section' => $entry['section']];
            }
            if (!empty($entry['data'])) {
                $action = 'sync-data';
                $opts = ['target' => $entry['target'], 'exclude' => $entry['exclude'], 'data' => 'all'];
            }
            if (!empty($entry['shared'])) {
                $action = 'sync-shared';
                $opts = ['target' => $entry['target'], 'shared' => 'all'];
            }
            if (!empty($entry['common'])) {
                $action = 'sync-common';
                $opts = ['target' => $entry['target'], 'common' => 'all'];
            }
            if (!empty($entry['quiet'])) {
                $opts['quiet'] = $entry['quiet'];
            }
            $schedule[] = [
                'time' => $this->dailyAt($t),
                'host' => $entry['source'],
                'user' => $this->app->cluster->getOption('sync_user'),
                'action' => $action,
                'options' => $opts
            ];
        }
        return $schedule;
    }

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
        \App :: log('pushed', 'cron/heartbeat');
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
        \App :: log('checked', 'cron/heartbeat');
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
        \App :: log('checked', 'cron/ping');
    }

    /**
     * Deploy configuration for system service
     * @argument string *service Service to deploy
     */
    function actionDeploy()
    {
        $this->app->cluster->startDeployService($this->arguments[0]);
    }

    /**
     * Execute deploy service if available. Should be executed as root
     */
    function actionProcessDeploy()
    {
        if (file_exists($this->app->cluster->getDeployFilename())) {
            foreach ($this->app->cluster->getOption('deploy') as $service => $conf) {
                if (file_exists($this->app->cluster->getDeployFilename($service))) {
                    $result = $this->app->cluster->processDeployService($service);
                    $this->stdout(implode(PHP_EOL, $result));
                    break;
                }
            }
            $this->app->triggerEvent(new \Skvn\Cluster\Events\CleanupDeploy());
        }
        \App :: log('done', 'cron/deploy');
    }

    /**
     * Cleanup from previous deploy attempt
     */
    function actionResetDeploy()
    {
        $this->app->cluster->triggerEvent(new \Skvn\Cluster\Events\CleanupDeploy());
    }


    /**
     * Scheduled rsync data sync
     */
    function actionSync()
    {
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
        if (!empty($action['section']) || !empty($action['data']) || !empty($action['shared']) || !empty($action['common'])) {
            if ($this->processSync($action)) {
                return;
            }
            $this->app->triggerEvent(new Log(['message' => "Unknown result", 'category' => 'dfs_sync']));
            return;
        }
        $this->app->triggerEvent(new Log(['message' => "No action", 'category' => 'dfs_sync']));
    }

    function actionSyncSection()
    {
        return $this->processSync($this->options);
    }

    function actionSyncData()
    {
        $opts = $this->options;
        if (!empty($opts['exclude'])) {
            $opts['exclude'] = explode(',', $opts['exclude']);
        }
        return $this->processSync($opts);
    }

    function actionSyncShared()
    {
        return $this->processSync($this->options);
    }


    function actionSyncCommon()
    {
        return $this->processSync($this->options);
    }

    private function processSync($args)
    {
        $startTime = time();
        $this->app->triggerEvent(new Log(['message' => "Syncing", 'category' => 'dfs_sync', 'info' => $args]));
        if (empty($args['target'])) {
            throw new ClusterException('Target not defined');
        }
        $t = microtime(true);
        $this->app->db->disconnect();
        $host = $this->app->cluster->getHostById($args['target']);
        if (!empty($args['section'])) {
            $this->app->triggerEvent(new ValidateSection($args));
            $command = $this->createSyncSectionCommand($host, $args['section'], $args);
            $this->stdout('Section ' . $args['section'] . ' copied to node ' . $args['target']);
        } elseif (!empty($args['data'])) {
            $exclude = $args['exclude'] ?? [];
            if (empty($exclude)) {
                $exclude = [];
            }
            $command = $this->createSyncDataCommand($host, $exclude, $args);
            $this->stdout('Full dataset copied to node ' . $args['target']);
            $this->stdout('Sections ' . implode(', ', $exclude) . 'excluded');
        } elseif (!empty($args['shared'])) {
            $command = $this->createSyncSharedCommand($host, $args);
            $this->stdout('Shared data copied to node ' . $args['target']);
        } elseif (!empty($args['common'])) {
            foreach ($this->app->cluster->getOption('sync') as $part => $path) {
                $this->stdout('Common part ' . $part . ' copied to node ' . $args['target']);
                $command = $this->createSyncCommonCommand($host, $path, $part, $args);
                $this->stdout($command);
                $result = [];
                exec($command, $result);
                $this->stdout('Result: ');
                $this->stdout($result);
                $this->stdout('');
            }
            $command = null;
        } else {
            throw new ClusterException('Nothing to sync');
        }

        if (!empty($command)) {
            exec($command, $result);
            $this->stdout($command);
            $this->stdout('Result: ');
            $this->stdout($result);
            $this->stdout('');
        }
        $this->stdout('Process done in ' . round(microtime(true)-$t, 1) . ' seconds');
        $this->app->triggerEvent(new Log([
            'message' => "Synced",
            'category' => 'dfs_sync',
            'result' => $this->strings,
            'startTime' => $startTime,
            'info' => $args,
            'time' => round(microtime(true) - $t, 1)
        ]));
        $this->mailSubject = 'DSF SYNC: ' . json_encode($args);
    }

    private function createSyncSectionCommand($targetHost, $section, $args = [])
    {
        $command = $this->app->cluster->getOption('rsync_command') . '  -a -h -L -k -K --delete --stats --partial ';
        $command .= $this->app->getPath($this->app->cluster->getOption("sections_path")) . "/" . $section . "/ ";
        $command .= $targetHost['img'] . "::section" . $section . ' 2>&1';
        return $command;
    }

    private function createSyncDataCommand($targetHost, $exclude, $args = [])
    {
        $command = $this->app->cluster->getOption('rsync_command') . '  -a -h -L -k -K --delete --stats --partial ';
        if (!empty($exclude)) {
            foreach ($exclude ?? [] as $ex) {
                $command .= ' --exclude=/' . $ex . ' ';
            }
        }
        $command .= $this->app->getPath($this->app->cluster->getOption("sections_path")) . "/ ";
        $command .= $targetHost['img'] . "::data 2>&1";
        return $command;
    }

    private function createSyncSharedCommand($targetHost, $args = [])
    {
        $command = $this->app->cluster->getOption('rsync_command') . '  -a -h -L --stats --partial ';
        $command .= $this->app->getPath($this->app->cluster->getOption('shared_path')) . '/ ';
        $command .= $targetHost['img'] . "::shared  2>&1";
        return $command;
    }

    private function createSyncCommonCommand($targetHost, $path, $name, $args = [])
    {
        
        $command = $this->app->cluster->getOption('rsync_command') . '  -a -h ' . $this->app->cluster->getOption('sync_common_links_args') . ' --delete --stats --partial ';
        $command .= $path . "/ ";
        $command .= $targetHost['img'] . "::common_" . $name . ' 2>&1';
        return $command;
    }






}