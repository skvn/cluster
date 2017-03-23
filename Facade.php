<?php

namespace Skvn\Cluster;

use Skvn\Base\Facade as BaseFacade;

class Facade extends BaseFacade
{
    protected static function getFacadeTarget()
    {
        return 'cluster';
    }
}