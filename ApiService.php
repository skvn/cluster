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

        $file = $this->app->getPath('@root/' . $data['file']);
        if (file_exists(dirname($file))) {
            mkdir(dirname($file), 0777, true);
        }
        $host = $this->app->cluster->getHostById($data['host_id']);
        $url = $this->app->cluster->getFileUrl($data['file'], $host);
        $fp = fopen($file, 'w');
        try {
            $this->app->urlLoader->load($url, [], ['returntransfer' => false, 'file' => $fp]);
            $this->app->cluster->log('SAVED', $data['file']);
        }
        catch (\Exception $e) {
            $this->app->cluster->log('SAVE FAILED', $data['file'], ['error' => $e->getMessage()]);
            unlink($file);
        }
        fclose($fp);
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