package pq

import (
	"database/sql"
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
