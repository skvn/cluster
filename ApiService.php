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

    }

}