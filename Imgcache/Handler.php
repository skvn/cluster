<?php

namespace Skvn\Cluster\Imgcache;


use Skvn\Base\Traits\AppHolder;
use Skvn\Cluster\Exceptions\ImgcacheException;

abstract class Handler
{
    use AppHolder;

    protected $config;
    protected $name;
    protected $path;

    public $distributed = false;

    function __construct($config, $name)
    {
        $this->config = $config;
        $this->name = $name;
        $this->path = $this->config['path'] . '/' . $name;
    }

    abstract function getUrlByArgs($args);
    abstract function getArgsByUrl($url);
    abstract function cache($args);
    abstract function flush($args);

    function validate($args)
    {
        return true;
    }

    function usage()
    {
        return '';
    }

    function getDistributedKey($args)
    {
        return 0;
    }

    function getLastChanged($args)
    {
        return 0;
    }

    protected function checkSourceImage($filename)
    {
        if (!file_exists($filename) || !is_file($filename)) {
            throw new ImgcacheException($filename . ' does not exist');
        }
        $size = getimagesize($filename);
        if ($size === false) {
            throw new ImgcacheException($filename . ' not a image');
        }
    }

    protected function getCachePrefix($hash, $level = 2, $replaceAd = false)
    {
        $parts = [];
        for ($i=0; $i<$level; $i++) {
            $part = substr($hash, $i*2, 2);
            if ($replaceAd) {
                $part = str_replace('ad', 'bd', $part);
            }
            $parts[] = $part;
        }
        return implode('/', $parts);
    }

    protected function getTruncatedHash($hash, $level)
    {
        return substr($hash, $level*2);
    }
}