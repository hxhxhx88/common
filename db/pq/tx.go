package pq

import (
	"database/sql"

	"github.com/golang/glog"
)

// Transaction ...
type Transaction struct {
	BatchExec
	tx *sql.Tx
}

// NewTransaction ...
func NewTransaction(db *sql.DB) (*Transaction, error) {
	t, err := db.Begin()
	if err != nil {
		return nil, err
	}

	tx := &Transaction{
		tx: t,
	}

	return tx, nil
}

// Exec ...
func (b *Transaction) Exec() (err error) {
	defer func() {
		if err != nil {
			b.tx.Rollback()
		}
	}()

	for _, ex := range b.execs {
		err = ex.exec(b.tx)
		if err != nil {
			return
		}
	}

	// clear executed sqls
	b.execs = []*Exec{}

	return
}

// Commit ...
func (b *Transaction) Commit() (err error) {
	defer func() {
		if err != nil {
			b.tx.Rollback()
		}
	}()

	// exec tail sqls
	err = b.Exec()
	if err != nil {
		return
	}

	// commit
	err = b.tx.Commit()
	if err != nil {
		return
	}

	return
}

// WithTransaction ...
func WithTransaction(db *sql.DB, queries func(tx *sql.Tx) (bool, error)) (err error) {
	var abort bool

	tx, err := db.Begin()
	if err != nil {
		glog.Error(err)
		return
	}
	defer func() {
		if err != nil || abort {
			tx.Rollback()
			return
		}
		if err = tx.Commit(); err != nil {
			glog.Error(err)
			tx.Rollback()
			return
		}
	}()

	abort, err = queries(tx)
	if err != nil {
		glog.Error(err)
		return
	}

	return
}
