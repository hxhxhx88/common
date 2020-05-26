package pq

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"
)

// TableName ...
type TableName string

// ColumnName ...
type ColumnName string

// Column ...
type Column struct {
	Table TableName
	Name  ColumnName
}

// InsertOption ...
type InsertOption struct {
	OnConflictDoNothing bool

	// Key is the field of inserting table.
	// Value is the field of reference table.
	// If provided, checks are performed before inserting.
	ForeignKeys map[ColumnName]Column
}

// BatchInsert ...
func BatchInsert(db *sql.DB, table TableName, records []Record) (ids []int, err error) {
	return BatchInsertWithOption(db, table, records, InsertOption{})
}

// BatchInsertWithOption ...
func BatchInsertWithOption(db *sql.DB, table TableName, records []Record, opt InsertOption) (ids []int, err error) {
	err = WithTransaction(db, func(tx *sql.Tx) (err error) {
		ids, err = BatchInsertTransaction(tx, table, records, opt)
		return
	})
	if err != nil {
		glog.Error(err)
		return
	}
	return
}

// PlaceholderLimit tells the maximal number of placeholders in a PostgreSQL statement.
const PlaceholderLimit = 65535

// BatchInsertTransaction ...
func BatchInsertTransaction(tx *sql.Tx, table TableName, records []Record, opt InsertOption) (ids []int, err error) {
	if len(records) == 0 {
		return
	}
	rec := records[0]
	cols := MapColumn(rec)
	if len(cols) == 0 {
		err = fmt.Errorf("missing columns")
		glog.Error(err)
		return
	}

	batchSize := PlaceholderLimit / len(cols)
	numBatch := len(records) / batchSize
	if len(records)%batchSize > 0 {
		numBatch++
	}

	for bidx := 0; bidx < numBatch; bidx++ {
		glog.Infof("inserting batch %v/%v", bidx+1, numBatch)

		m := bidx * batchSize
		n := (bidx + 1) * batchSize
		if n > len(records) {
			n = len(records)
		}
		recs := records[m:n]

		currIDs, e := insertBatch(tx, table, recs, opt)
		if e != nil {
			err = e
			glog.Error(err)
			return
		}
		ids = append(ids, currIDs...)
	}

	return
}

func insertBatch(tx *sql.Tx, table TableName, records []Record, opt InsertOption) (ids []int, err error) {
	query, args, empty, err := MakeBatchInsertQuery(table, records, opt)
	if err != nil {
		glog.Error(err)
		return
	}
	if empty {
		return
	}

	// exec
	rows, err := tx.Query(query, args...)
	if err != nil {
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

	return
}

// MakeBatchInsertQuery ...
func MakeBatchInsertQuery(table TableName, records []Record, opt InsertOption) (query string, args []interface{}, empty bool, err error) {
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

	// will use different way to handle foreign key case
	hasForeignKey := len(opt.ForeignKeys) > 0

	// make placeholders and args
	var rows []string
	for _, vs := range values {
		var phds []string
		for _, f := range fields {
			v := vs[f]

			var suffix string
			if hasForeignKey {
				switch v.(type) {
				case int, int8, int16, int32, uint, uint8, uint16, uint32, uint64, *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32:
					suffix = "::integer"
				case int64, *uint64:
					suffix = "::bigint"
				case time.Time, *time.Time:
					suffix = "::timestamp without time zone"
				}
			}

			args = append(args, v)
			phds = append(phds, fmt.Sprintf("$%d%s", len(args), suffix))
		}
		row := "(" + strings.Join(phds, ",") + ")"
		rows = append(rows, row)
	}

	onConflict := ""
	if opt.OnConflictDoNothing {
		onConflict = " ON CONFLICT DO NOTHING "
	}

	// make sql
	if len(opt.ForeignKeys) == 0 {
		// if no foreign key is provided, things are easy.
		query = fmt.Sprintf(`INSERT INTO %s (%s) VALUES %s %s RETURNING id`,
			table,
			strings.Join(fields, ","),
			strings.Join(rows, ","),
			onConflict,
		)
		return
	}

	// make query checking foreign keys
	// https://stackoverflow.com/a/45229846

	var existClauses []string
	for field, ref := range opt.ForeignKeys {
		clause := fmt.Sprintf(`(
			EXISTS (
				SELECT 1 FROM %s AS ref WHERE ref.%s = vs.%s
			)
		)`, ref.Table, ref.Name, field)
		existClauses = append(existClauses, clause)
	}
	fieldStr := strings.Join(fields, ",")

	query = fmt.Sprintf(`
	INSERT INTO %s
		(%s)
	SELECT
		*
	FROM (
		VALUES %s
	) AS vs(%s)
	WHERE %s
	%s
	RETURNING id`,
		table,
		fieldStr,
		strings.Join(rows, ","),
		fieldStr,
		strings.Join(existClauses, " AND "),
		onConflict,
	)

	return
}
