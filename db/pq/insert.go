package pq

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/golang/glog"
)

// InsertOption ...
type InsertOption struct {
	OnConflictDoNothing bool
}

// BatchInsert ...
func BatchInsert(db *sql.DB, table string, records []Record) (ids []int, err error) {
	return BatchInsertWithOption(db, table, records, InsertOption{})
}

// BatchInsertWithOption ...
func BatchInsertWithOption(db *sql.DB, table string, records []Record, opt InsertOption) (ids []int, err error) {
	tx, err := db.Begin()
	if err != nil {
		glog.Error(err)
		return
	}
	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		if err = tx.Commit(); err != nil {
			glog.Error(err)
			tx.Rollback()
			return
		}
	}()

	ids, err = BatchInsertTransaction(tx, table, records, opt)
	if err != nil {
		glog.Error(err)
		return
	}

	return
}

// PlaceholderLimit tells the maximal number of placeholders in a PostgreSQL statement.
const PlaceholderLimit = 65535

// BatchInsertTransaction ...
func BatchInsertTransaction(tx *sql.Tx, table string, records []Record, opt InsertOption) (ids []int, err error) {
	if len(records) == 0 {
		return
	}
	rec := records[0]
	cols := MapColumn(rec)
	batchSize := PlaceholderLimit / len(cols)
	numBatch := len(records) / batchSize
	if len(records)%batchSize > 0 {
		numBatch++
	}

	for i := 0; i < numBatch; i++ {
		m := i * batchSize
		n := (i + 1) * batchSize
		if n > len(records) {
			n = len(records)
		}
		recs := records[m:n]

		query, args, empty, e := MakeBatchInsertQuery(table, recs, opt)
		if e != nil {
			err = e
			glog.Error(err)
			return
		}
		if empty {
			return
		}

		// exec
		rows, e := tx.Query(query, args...)
		if e != nil {
			err = e
			glog.Error(err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var id int
			if err = rows.Scan(&id); err != nil {
				glog.Error(err)
				return
			}
			ids = append(ids, id)
		}
	}

	return
}

// MakeBatchInsertQuery ...
func MakeBatchInsertQuery(table string, records []Record, opt InsertOption) (query string, args []interface{}, empty bool, err error) {
	var fields []string
	var values []map[string]interface{}
	for _, rec := range records {
		cols := MapColumn(rec)
		values = append(values, cols)
	}
	if len(values) == 0 {
		empty = true
		return
	}

	// use the first record as reference to determine the order of fields
	ref := values[0]
	for f := range ref {
		fields = append(fields, f)
	}
	if len(fields) == 0 {
		err = fmt.Errorf("empty inserting fields")
		glog.Error(err)
		return
	}

	// make placeholders and args
	var rows []string
	for _, vs := range values {
		var phds []string
		for _, f := range fields {
			args = append(args, vs[f])
			phds = append(phds, fmt.Sprintf("$%d", len(args)))
		}
		row := "(" + strings.Join(phds, ",") + ")"
		rows = append(rows, row)
	}

	onConflict := ""
	if opt.OnConflictDoNothing {
		onConflict = " ON CONFLICT DO NOTHING "
	}

	// make sql
	query = fmt.Sprintf(`INSERT INTO %s (%s) VALUES %s %s RETURNING id`,
		table,
		strings.Join(fields, ","),
		strings.Join(rows, ","),
		onConflict,
	)

	return
}
