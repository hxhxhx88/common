package pq

import (
	"database/sql"
)

// Exec ...
type Exec struct {
	sql       string
	multiRows bool
	args      []interface{}
	dest      []interface{}
}

// SetArgs ...
func (ex *Exec) SetArgs(args ...interface{}) *Exec {
	ex.args = args
	return ex
}

// SetScans ...
func (ex *Exec) SetScans(dest ...interface{}) *Exec {
	ex.dest = dest
	return ex
}

// AddScan ...
func (ex *Exec) AddScan(dest interface{}) *Exec {
	ex.dest = append(ex.dest, dest)
	return ex
}

// SetMultiRows ...
func (ex *Exec) SetMultiRows() *Exec {
	ex.multiRows = true
	return ex
}

func (ex *Exec) exec(tx *sql.Tx) (err error) {
	stt, e := tx.Prepare(ex.sql)
	if e != nil {
		err = e
		return
	}
	defer stt.Close()

	if len(ex.dest) == 0 {
		if _, e := stt.Exec(ex.args...); e != nil {
			err = e
			return
		}
	} else if !ex.multiRows {
		row := stt.QueryRow(ex.args...)
		if e := row.Scan(ex.dest...); e != nil {
			err = e
			return
		}
	} else {
		rows, e := stt.Query(ex.args...)
		if e != nil {
			err = e
			return
		}
		defer rows.Close()

		var i int
		for rows.Next() && i < len(ex.dest) {
			if err = rows.Scan(ex.dest[i]); err != nil {
				return
			}
			i++
		}
	}

	return
}
