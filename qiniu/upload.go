package qiniu

import (
	"bytes"
	"context"
	"sync"

	"github.com/qiniu/api.v7/auth/qbox"
	"github.com/qiniu/api.v7/storage"
)

//Item ...
type Item struct {
	Data []byte
	Name string
}

//Result ...
type Result struct {
	Name  string
	URL   string
	Error error
}

// Upload ...
func (c *Client) Upload(item Item) (result Result) {
	result.Name = item.Name

	// make qiniu config
	var conf storage.Config
	conf.Zone = &c.zone
	conf.UseHTTPS = c.useHTTPS
	conf.UseCdnDomains = c.useCDNDomains

	// make upload token
	putPolicy := storage.PutPolicy{
		Scope: c.bucket,
	}
	mac := qbox.NewMac(c.accessKey, c.secretKey)
	upToken := putPolicy.UploadToken(mac)

	// make uploader
	formUploader := storage.NewFormUploader(&conf)

	// upload
	ret := storage.PutRet{}
	dataLen := int64(len(item.Data))
	err := formUploader.Put(context.Background(), &ret, upToken, item.Name, bytes.NewReader(item.Data), dataLen, nil)
	if err != nil {
		result.Error = err
		return
	}

	result.URL = storage.MakePublicURL(c.domain, item.Name)
	return
}

// BatchUpload batch uploads multiple items.
// The results may in different order to the input. Please use the Name field of result to identify.
func (c *Client) BatchUpload(items []Item, nj int) (results []Result) {
	itemChan := make(chan Item)
	go func() {
		for _, item := range items {
			itemChan <- item
		}
		close(itemChan)
	}()

	if len(items) < nj {
		nj = len(items)
	}

	var wg sync.WaitGroup
	for i := 0; i < nj; i++ {
		wg.Add(1)
		go func() {
			for item := range itemChan {
				res := c.Upload(item)
				results = append(results, res)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	return
}
