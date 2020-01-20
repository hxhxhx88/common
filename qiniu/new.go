package qiniu

import (
	"fmt"

	"github.com/qiniu/api.v7/storage"
)

// Client ...
type Client struct {
	accessKey     string
	secretKey     string
	zone          storage.Zone
	useHTTPS      bool
	useCDNDomains bool
	bucket        string
	domain        string
}

// Zone ...
type Zone int

// ...
const (
	ZoneSouth Zone = iota
	ZoneEast
	ZoneNorth
	ZoneUSA
	ZoneSingapo
)

// New ...
func New(accessKey string, secretKey string, zone Zone, bucket string, domain string) (*Client, error) {
	var client Client
	client.accessKey = accessKey
	client.secretKey = secretKey
	client.bucket = bucket
	client.domain = domain

	switch zone {
	case ZoneSouth:
		client.zone = storage.ZoneHuanan
	case ZoneNorth:
		client.zone = storage.ZoneHuabei
	case ZoneEast:
		client.zone = storage.ZoneHuadong
	case ZoneUSA:
		client.zone = storage.ZoneBeimei
	case ZoneSingapo:
		client.zone = storage.ZoneXinjiapo
	default:
		return nil, fmt.Errorf("unrecognized zone")
	}

	return &client, nil
}

// New2 ...
func New2(c Conf) (*Client, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	var zone Zone
	switch c.Zone {
	case "east":
		zone = ZoneEast
	case "south":
		zone = ZoneSouth
	case "north":
		zone = ZoneNorth
	case "usa":
		zone = ZoneUSA
	case "singapo":
		zone = ZoneSingapo
	}
	return New(c.AccessKey, c.SecretKey, zone, c.Bucket, c.Domain)
}

//UseHTTPS ...
func (c *Client) UseHTTPS() {
	c.useHTTPS = true
}

//UseCDNDomains ...
func (c *Client) UseCDNDomains() {
	c.useCDNDomains = true
}
