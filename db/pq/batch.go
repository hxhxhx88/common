package pq

import (
	"database/sql"
	"fmt"

	"github.com/golang/glog"
)

// BatchExec ...
type BatchExec struct {
	execs []*Exec
}

// NewBatchExec ...
func NewBatchExec() *BatchExec {
	var b BatchExec
	return &b
}

// Add ...
func (b *BatchExec) Add(sql string) *Exec {
	var e Exec
	e.sql = sql
	b.execs = append(b.execs, &e)
	return &e
}

// Exec ...
func (b *BatchExec) Exec(db *sql.DB) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	for _, ex := range b.execs {
		err = ex.exec(tx)
		if err != nil {
			glog.Error(err)
			return
		}
	}

	return
}

// InsertionPrepare ...
type InsertionPrepare struct {
	fields []string
	phds   []string
	args   []interface{}
}

// AddValue ...
func (p *InsertionPrepare) AddValue(field string, value interface{}) {
	p.fields = append(p.fields, field)
	p.phds = append(p.phds, fmt.Sprintf("$%v", len(p.fields)))
	p.args = append(p.args, value)
}

// AddValueWhen ...
func (p *InsertionPrepare) AddValueWhen(field string, value interface{}, check bool) {
	if !check {
		return
	}
	p.AddValue(field, value)
}

// Fields ...
func (p *InsertionPrepare) Fields() []string {
	return p.fields
}

// Placeholders ...
func (p *InsertionPrepare) Placeholders() []string {
	return p.phds
}

// Arguments ...
func (p *InsertionPrepare) Arguments() []interface{} {
	return p.args
}
