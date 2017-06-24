<?php

namespace Skvn\Cluster\Imgcache;

use Skvn\Cluster\Exceptions\ImgcacheException;
use Skvn\Base\Helpers\File;

class ResizeHandler extends Handler
{

    function getTargetPathByArgs($args)
    {
        $this->checkSourceImage($args['path']);
        $hash = md5($args['path']);
        $ext = File :: getExtension($args['path']);
        $size = explode('x', $args['size']);
        $cached = $this->getCachePrefix($hash, 2) . '/' .
                    $size[0] . 's' . $size[1] . '-' .
                    $this->getTruncatedHash($hash, 2) . '.' . $ext;
        $target = $this->path . '/' . $cached;
        if (!file_exists(dirname($target))) {
            File :: safeMkdir(dirname($target));
        }
        if (!file_exists($target)) {
            $index = file_exists(dirname($target) . '/.index') ? parse_ini_file(dirname($target) . '/.index') : [];
            if (!isset($index[$hash])) {
                error_log($hash . ' = "' . $args['path'] . '"' . PHP_EOL, 3, dirname($target) . '/.index');
            }
        }
        return $cached;
    }

    function getArgsByTargetPath($url)
    {
        $args = ['cached' => $url];
        $segments = explode('/', $url);
        $filename = array_pop($segments);
        $hashParts = [];
        while (count($segments)) {
            $hashParts[] = array_shift($segments);
        }
        $filename = preg_replace('#\..+$#', '', $filename);
        $fileParts = explode('-', $filename);
        $args['size'] = str_replace('s', 'x', $fileParts[0]);
        $hashParts[] = $fileParts[1];
        $args['hash'] = implode('', $hashParts);
        $target = $this->path . '/' . $url;
        $index = file_exists(dirname($target) . '/.index') ? parse_ini_file(dirname($target) . '/.index') : [];
        if (isset($index[$args['hash']])) {
            $args['path'] = $index[$args['hash']];
        } else {
            throw new ImgcacheException('Index for ' . $args['hash'] . ' not found');
        }
        return $args;
    }

    function buildTargetImage($args)
    {
        $this->checkSourceImage($args['path']);
        $class = $this->imgWorker;
        $img = new $class($args['path']);
        return $img;
    }

    function removeTarget($args)
    {

    }

    function validate($args)
    {
        if (empty($args['path'])) {
            return false;
        }
        if (empty($args['size'])) {
            return false;
        }
        $sizeParts = explode('x', $args['size']);
        if (count($sizeParts) < 2) {
            return false;
        }
        return true;
    }

    function usage()
    {
        return 'size=WxH required, path required';
    }


}