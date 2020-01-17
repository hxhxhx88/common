package email

import (
	"fmt"

	"gopkg.in/gomail.v2"
)

//Conf ...
type Conf struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// Validate ...
func (c Conf) Validate() error {
	if c.Username == "" {
		return fmt.Errorf("missing Username")
	}
	if c.Password == "" {
		return fmt.Errorf("missing Password")
	}
	if c.Host == "" {
		return fmt.Errorf("missing Host")
	}
	if c.Port == 0 {
		return fmt.Errorf("missing Port")
	}
	return nil
}

//Client ...
type Client struct {
	dialer *gomail.Dialer
	from   string
}

// NewClient ...
func NewClient(conf Conf) *Client {
	d := gomail.NewDialer(
		conf.Host,
		conf.Port,
		conf.Username,
		conf.Password,
	)

	var e Client
	e.dialer = d
	e.from = conf.Username

	return &e
}

// Send ...
func (e *Client) Send(subject string, content string, to string) error {
	m := gomail.NewMessage()
	m.SetHeader("From", e.from)
	m.SetHeader("To", to)
	m.SetHeader("Subject", subject)
	m.SetBody("text/html", content)
	return e.dialer.DialAndSend(m)
}
