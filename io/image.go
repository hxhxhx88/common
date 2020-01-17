package io

import (
	"image"
	"image/jpeg"
	"image/png"
	"os"

	"github.com/golang/glog"
)

// SaveJPEG ...
func SaveJPEG(im image.Image, quality int, savePath string) (err error) {
	imFile, err := os.Create(savePath)
	if err != nil {
		glog.Error(err)
		return
	}
	defer imFile.Close()
	err = jpeg.Encode(imFile, im, &jpeg.Options{Quality: quality})
	if err != nil {
		glog.Error(err)
		return
	}
	return
}

// SavePNG ...
func SavePNG(im image.Image, savePath string) (err error) {
	imFile, err := os.Create(savePath)
	if err != nil {
		glog.Error(err)
		return
	}
	defer imFile.Close()
	err = png.Encode(imFile, im)
	if err != nil {
		glog.Error(err)
		return
	}
	return
}
