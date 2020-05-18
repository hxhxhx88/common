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
		if reflect.Zero(field.Type).Interface() == value.Interface() {
			// ignore fields with empty value
			continue
		}

		table[dbColumn] = value.Interface()
	}

	return table
}
