<?php

namespace Skvn\Cluster\Imgcache;

class ImagickWrap extends \Imagick
{
    static function createImage($w, $h, $bg, $fmt="png")
    {
        $img = new static();
        $img->newImage($w, $h, new \ImagickPixel($bg));
        $img->setImageFormat($fmt);
        return $img;
    }

    function smartResize($w, $h, $args = [])
    {
        $this->scaleImage($w, $h, true);
    }

    function getImageColorspaceName()
    {
        return static :: colorspaceName($this->getImageColorspace());
    }

    static function colorspaceName($colorspace)
    {
        switch ($colorspace)
        {
            case static::COLORSPACE_UNDEFINED:
            default:
                return "UNKNOWN";
                break;
            case static::COLORSPACE_RGB:
                return "RGB";
                break;
            case static::COLORSPACE_GRAY:
                return "GRAY";
                break;
            case static::COLORSPACE_TRANSPARENT:
                return "TRANSPARENT";
                break;
            case static::COLORSPACE_OHTA:
                return "OHTA";
                break;
            case static::COLORSPACE_LAB:
                return "LAB";
                break;
            case static::COLORSPACE_XYZ:
                return "XYZ";
                break;
            case static::COLORSPACE_YCBCR:
                return "YCBCR";
                break;
            case static::COLORSPACE_YCC:
                return "YCC";
                break;
            case static::COLORSPACE_YIQ:
                return "YIQ";
                break;
            case static::COLORSPACE_YPBPR:
                return "YPBPR";
                break;
            case static::COLORSPACE_YUV:
                return "YUV";
                break;
            case static::COLORSPACE_CMYK:
                return "CMYK";
                break;
            case static::COLORSPACE_SRGB:
                return "SRGB";
                break;
            case static::COLORSPACE_HSB:
                return "HSB";
                break;
            case static::COLORSPACE_HSL:
                return "HSL";
                break;
            case static::COLORSPACE_HWB:
                return "HWB";
                break;
            case static::COLORSPACE_REC601LUMA:
                return "REC601LUMA";
                break;
            case static::COLORSPACE_REC709LUMA:
                return "REC709LUMA";
                break;
            case static::COLORSPACE_LOG:
                return "LOG";
                break;
            case static::COLORSPACE_CMY:
                return "CMY";
                break;
        }
    }


}