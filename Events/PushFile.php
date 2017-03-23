<?php

namespace Skvn\Cluster\Events;

use Skvn\Event\Event as BaseEvent;
use Skvn\Event\Contracts\BackgroundEvent;
use Skvn\Event\Contracts\SelfHandlingEvent;

class PushFile extends BaseEvent implements BackgroundEvent, SelfHandlingEvent
{

    function queue()
    {
        return "files";
    }

    function handle()
    {
        $this->app->cluster->pushFileSync($this->file);
        return 'Dfs pushed file ' . $this->file;
    }
}