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
    }

}