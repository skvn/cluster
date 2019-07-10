<?php

namespace Skvn\Cluster\Imgcache;


use Skvn\Cluster\Exceptions\ImgcacheException;

abstract class Handler
{

    public $path;
    public $imgWorker = ImagickWrap :: class;

    public $distributed = false;


    abstract function getTargetPathByArgs($args);
    abstract function getArgsByTargetPath($url);
    abstract function buildTargetImage($args);
    abstract function removeTarget($args);

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
}