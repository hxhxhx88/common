package pq

import (
	"reflect"
)

// Record ...
type Record interface{}

// MapColumn turns a struct into a database record, e.g.
// 	  struct Record {
// 	 	Name string `db:name`
// 		Gender bool `db:gender`
// 		Internal string // field withtout `db:"xxx"` will be ignored
// 		Empty *string `db:"nil"` // empty value will be ignored
// 		private string `db:"ineffective` // private field will be ignored
//    }
// will result in a map
//    name -> XXX, gender -> XXX
func MapColumn(r Record) map[string]interface{} {
	var opt MapColumnOption
	return MapColumnWithOption(r, opt)
}

// MapColumnOption ...
type MapColumnOption struct {
	// Postgres requires to wrap an array in a `pq.Array`.
	// For this method to be general, we provide an option for the caller to deal with this.
	WrapArray func(a interface{}) interface{}

	// Columns whose value is empty should also be inserted instead of omitted.
	// For example, for an NOT-NULL integer column whose being 0 is perfect valid, we should add it to this option.
	KeepEmptyValueColums []string
}

// MapColumnWithOption ...
func MapColumnWithOption(r Record, opt MapColumnOption) map[string]interface{} {
	keepEmptyValueCol := make(map[string]bool)
	for _, col := range opt.KeepEmptyValueColums {
		keepEmptyValueCol[col] = true
	}

	table := make(map[string]interface{})

	val := reflect.ValueOf(r)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		dbColumn := field.Tag.Get("db")
		if dbColumn == "" {
			// ignore field without `db` tag
			continue
		}

		value := val.FieldByName(field.Name)
		if !value.CanInterface() {
			// ignore private fields
			continue
		}

		// can not use `reflect.Zero(field.Type).Interface() == value.Interface()` to tell if a slice is empty, thus we check by cases
		if value.Kind() == reflect.Slice {
			if value.Len() == 0 && !keepEmptyValueCol[dbColumn] {
				// empty slice
				continue
			}
			if opt.WrapArray != nil {
				table[dbColumn] = opt.WrapArray((value.Interface()))
			} else {
				// ignore slice
				continue
			}
		} else {
			if reflect.Zero(field.Type).Interface() == value.Interface() && !keepEmptyValueCol[dbColumn] {
				// ignore fields with empty value
				continue
			}
			table[dbColumn] = value.Interface()
		}
	}

	return table
}
