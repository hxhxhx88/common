package qiniu

import (
	"crypto/md5"
	"fmt"
	"runtime"

	"github.com/golang/glog"
)

// UploadMD5Naming ...
func (c *Client) UploadMD5Naming(data []byte, prefix string) (url string, err error) {
	// upload file
	fileHash := dataMD5(data)
	fileName := fmt.Sprintf("%s/%s", prefix, fileHash)
	var item = Item{
		Data: data,
		Name: fileName,
	}

	res := c.Upload(item)
	if res.Error != nil {
		err = res.Error
		glog.Error(err)
		return
	}

	url = res.URL

	return
}

// BatchUploadMD5Naming ...
func (c *Client) BatchUploadMD5Naming(data [][]byte, prefix string) (urls []string, err error) {
	var items []Item

	var names []string
	for _, d := range data {
		fileHash := dataMD5(d)
		fileName := fmt.Sprintf("%s/%s", prefix, fileHash)
		names = append(names, fileName)

		var item = Item{
			Data: d,
			Name: fileName,
		}
		items = append(items, item)
	}

	// The returned urls may in different order. We use a lookup table to sort them.
	nameURL := make(map[string]string)

	results := c.BatchUpload(items, runtime.NumCPU())
	for _, res := range results {
		if res.Error != nil {
			err = res.Error
			return
		}
		nameURL[res.Name] = res.URL
	}

	for _, name := range names {
		urls = append(urls, nameURL[name])
	}

	return
}

func dataMD5(data []byte) string {
	t := md5.New()
	t.Write(data)
	return fmt.Sprintf("%x", t.Sum(nil))
}
