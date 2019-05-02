<?php

namespace Skvn\Cluster;

use Skvn\App\ApiService as BaseService;

class ApiService extends BaseService
{

    function getName()
    {
        return 'dfs';
    }

    function authorize($data)
    {
        if (!isset($data['host_id'])) {
            return false;
        }
        $host = $this->app->cluster->getHostById($data['host_id']);
        if ($host['ip'] != $this->app->request->getClientIp()) {
            return false;
        }

        return true;
    }

    function fetch($data)
    {
        if (empty($data['file'])) {
            throw new Exceptions\ClusterException('No filename to push');
        }
        $host = $this->app->cluster->getHostById($data['host_id']);
        return $this->app->cluster->fetchFileFromHost($data['file'], $host);
    }

    function delete($data)
    {
        if (empty($data['file'])) {
            throw new Exceptions\ClusterException('No filename to delete');
        }
        $file = $this->app->getPath('@root/' . $data['file']);
        if (file_exists($file)) {
            unlink($file);
            $this->app->cluster->log('DELETED', $data['file']);
        }
        return [];
    }

    function queue($data)
    {
        $queue = $this->app->cluster->getQueueForHost($data['host_id']);
        return $queue;
    }

    function update_client($data)
    {
        $data['id'] = $data['host_id'];
        unset($data['host_id']);
        unset($data['endpoint']);
        return ['result' => $this->app->cluster->updateClient($data)];
    }

    function triggerEvent($data)
    {
        $class = $data['event'];
        unset($data['event']);
        $this->app->triggerEvent(new $class($data));
    }

    function ping($data)
    {
        return true;
    }

    function storeDeployData($data)
    {
        if (empty($data['service'])) {
            throw new Exceptions\ClusterException('Service to deploy not defined');
        }
        $conf = $this->app->cluster->getOption('deploy');
        if (empty($conf[$data['service']])) {
            throw new Exceptions\ClusterException('Configuration for service ' . $data['service'] . ' not found');
        }
        if (file_exists($this->app->cluster->getDeployFilename())) {
            throw new Exceptions\ClusterException('Another deploy process is active');
        }
        mkdir($this->app->cluster->getDeployFilename(), 0777, true);
        foreach ($data['files'] ?? [] as $file => $content) {
            $path = $this->app->cluster->getDeployFilename($data['service'], $file);
            if (!file_exists(dirname($path))) {
                mkdir(dirname($path), 0777, true);
            }
            file_put_contents($path, base64_decode($content));
        }
        $this->app->triggerEvent(new \Skvn\Event\Events\NotifyProblem([
            'problem' => 'deploy-started',
            'host_id' => $this->app->cluster->getOption('my_id'),
            'service' => $data['service']
        ]));
    }

}