<?php

namespace Skvn\Cluster\Events;

use Skvn\Event\Event as BaseEvent;
use Skvn\Event\Contracts\BackgroundEvent;
use Skvn\Event\Contracts\SelfHandlingEvent;

class DeleteFile extends BaseEvent implements BackgroundEvent, SelfHandlingEvent
{

    function queue()
    {
        return "files";
    }

    function handle()
    {
        $this->app->cluster->deleteFileSync($this->file);
        return 'Dfs deleted file ' . $this->file;
    }
}