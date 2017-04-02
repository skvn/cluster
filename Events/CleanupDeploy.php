<?php

namespace Skvn\Cluster\Events;

use Skvn\Event\Event as BaseEvent;
use Skvn\Event\Contracts\SelfHandlingEvent;
use Skvn\Base\Helpers\File;

class CleanupDeploy extends BaseEvent implements SelfHandlingEvent
{


    function handle()
    {
        File :: rm($this->app->cluster->getDeployFilename());
    }
}