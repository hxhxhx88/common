package qiniu

import (
	"fmt"
)

// Conf ...
type Conf struct {
	AccessKey     string `yaml:"access_key"`
	SecretKey     string `yaml:"secret_key"`
	Zone          string `yaml:"zone"`
	UseHTTPS      bool   `yaml:"use_https"`
	UseCDNDomains bool   `yaml:"use_cdn"`
	Bucket        string `yaml:"bucket"`
	Domain        string `yaml:"domain"`
}

// Validate ...
func (c Conf) Validate() error {
	if c.AccessKey == "" {
		return fmt.Errorf("missing AccessKey")
	}
	if c.SecretKey == "" {
		return fmt.Errorf("missing SecretKey")
	}
	switch c.Zone {
	case "south", "east", "north", "usa", "singapo":
		// good
	default:
		return fmt.Errorf("invalid zone: %s", c.Zone)
	}
	if c.Bucket == "" {
		return fmt.Errorf("missing Bucket")
	}
	if c.Domain == "" {
		return fmt.Errorf("missing Domain")
	}
	return nil
}
