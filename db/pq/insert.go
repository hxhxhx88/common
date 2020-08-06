package pq

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/lib/pq"
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

// InsertInformation ...
type InsertInformation struct {
	BatchCount int
}

// InsertOption ...
type InsertOption struct {
	Abort                *bool
	OnConflict           string
	BeforeInsertCallback func(info InsertInformation)
	BatchCallback        func(batchIndex int)
	NoID                 bool

	// Key is the field of inserting table.
	// Value is the field of reference table.
	// If provided, checks are performed before inserting.
	ForeignKeys map[ColumnName]Column

	// Columns whose value is empty should also be inserted instead of omitted.
	// For example, for an NOT-NULL integer column whose being 0 is perfect valid, we should add it to this option.
	KeepEmptyValueColums []ColumnName
}

// BatchInsert ...
func BatchInsert(dbase *sql.DB, table TableName, records []Record) (ids []int, err error) {
	return BatchInsertWithOption(dbase, table, records, InsertOption{})
}

// BatchInsertWithOption ...
func BatchInsertWithOption(dbase *sql.DB, table TableName, records []Record, opt InsertOption) (ids []int, err error) {
	err = WithTransaction(dbase, func(tx *sql.Tx) (abort bool, err error) {
		ids, err = BatchInsertTransaction(tx, table, records, opt)
		if opt.Abort != nil {
			abort = *opt.Abort
		}
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

	colSet := make(map[string]bool)
	for _, rec := range records {
		cols := mapColumn(rec, opt)
		for col := range cols {
			colSet[col] = true
		}
	}
	if len(colSet) == 0 {
		err = fmt.Errorf("missing columns")
		glog.Error(err)
		return
	}
	var columns []string
	for col := range colSet {
		columns = append(columns, col)
	}

	batchSize := PlaceholderLimit / len(columns)
	numBatch := len(records) / batchSize
	if len(records)%batchSize > 0 {
		numBatch++
	}

	if opt.BeforeInsertCallback != nil {
		info := InsertInformation{
			BatchCount: numBatch,
		}
		opt.BeforeInsertCallback(info)
	}

	for bidx := 0; bidx < numBatch; bidx++ {
		if opt.Abort != nil && *opt.Abort {
			glog.Infof("inserting aborted")
			break
		}

		glog.Infof("inserting batch %v/%v", bidx+1, numBatch)

		m := bidx * batchSize
		n := (bidx + 1) * batchSize
		if n > len(records) {
			n = len(records)
		}
		recs := records[m:n]

		currIDs, e := insertBatch(tx, table, recs, columns, opt)
		if e != nil {
			err = e
			glog.Error(err)
			return
		}
		ids = append(ids, currIDs...)

		if opt.BatchCallback != nil {
			opt.BatchCallback(bidx)
		}
	}

	return
}

func insertBatch(tx *sql.Tx, table TableName, records []Record, columns []string, opt InsertOption) (ids []int, err error) {
	query, args, empty, err := MakeBatchInsertQuery(table, records, columns, opt)
	if err != nil {
		glog.Error(err)
		return
	}
	if empty {
		return
	}

	// exec
	if opt.NoID {
		_, err = tx.Exec(query, args...)
		if err != nil {
			glog.Error(err)
			return
		}
		return
	}

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
func MakeBatchInsertQuery(table TableName, records []Record, columns []string, opt InsertOption) (query string, args []interface{}, empty bool, err error) {
	if len(columns) == 0 {
		err = fmt.Errorf("empty inserting fields")
		glog.Error(err)
		return
	}

	var values []map[string]interface{}
	for _, rec := range records {
		cols := mapColumn(rec, opt)
		values = append(values, cols)
	}
	if len(values) == 0 {
		empty = true
		return
	}

	// will use different way to handle foreign key case
	hasForeignKey := len(opt.ForeignKeys) > 0

	// make placeholders and args
	var rows []string
	for _, vs := range values {
		var phds []string
		for _, f := range columns {
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
	onConflict := " " + opt.OnConflict + " "

	returning := "RETURNING id"
	if opt.NoID {
		returning = ""
	}

	// make sql
	if len(opt.ForeignKeys) == 0 {
		// if no foreign key is provided, things are easy.
		query = fmt.Sprintf(`INSERT INTO %s (%s) VALUES %s %s %s`,
			table,
			strings.Join(columns, ","),
			strings.Join(rows, ","),
			onConflict,
			returning,
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
	fieldStr := strings.Join(columns, ",")

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
	%s`,
		table,
		fieldStr,
		strings.Join(rows, ","),
		fieldStr,
		strings.Join(existClauses, " AND "),
		onConflict,
		returning,
	)

	return
}

func mapColumn(r Record, opt InsertOption) map[string]interface{} {
	var keepEmptyValueCols []string
	for _, c := range opt.KeepEmptyValueColums {
		keepEmptyValueCols = append(keepEmptyValueCols, string(c))
	}

	return MapColumnWithOption(r, MapColumnOption{
		WrapArray: func(a interface{}) interface{} {
			if _, ok := a.([]byte); ok {
				// `[]byte` corresponds to `bytea` in PostgreSQL and we should do nothing
				return a
			}
			return pq.Array(a)
		},
		KeepEmptyValueColums: keepEmptyValueCols,
	})
}
