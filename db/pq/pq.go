package pq

import (
	"database/sql"
	"fmt"
	"strings"

	// ...
	_ "github.com/lib/pq"
)

// Conf ...
type Conf struct {
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
	Host     string `yaml:"host,omitempty"`
	Port     int    `yaml:"port,omitempty"`
	Database string `yaml:"database,omitempty"`
}

// Validate ...
func (c Conf) Validate() error {
	if c.Username == "" {
		return fmt.Errorf("missing Username")
	}
	if c.Host == "" {
		return fmt.Errorf("missing Host")
	}
	if c.Port < 0 {
		return fmt.Errorf("illegal Port")
	}
	if c.Database == "" {
		return fmt.Errorf("Missing Database")
	}
	return nil
}

// New ...
func New(conf Conf) (db *sql.DB, err error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		conf.Username,
		conf.Password,
		conf.Host,
		conf.Port,
		conf.Database,
	)
	return sql.Open("postgres", connStr)
}

// IsDuplicatedKeyError ...
func IsDuplicatedKeyError(err error, keyName string) bool {
	return strings.Contains(err.Error(), fmt.Sprintf(`pq: duplicate key value violates unique constraint "%s"`, keyName))
}
