package web

import (
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/golang/glog"
)

// ParseQueryString turns the query string of a request into a JSON struct.
func ParseQueryString(r *http.Request, targetPtr interface{}) (err error) {
	typ := reflect.TypeOf(targetPtr).Elem()
	val := reflect.ValueOf(targetPtr).Elem()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		column := field.Tag.Get("json")
		if column == "" || column == "-" {
			continue
		}
		column = strings.TrimSuffix(column, ",omitempty")

		f := val.Field(i)
		if !(f.IsValid() && f.CanSet()) {
			glog.Warningf("skipped field %v", column)
			continue
		}

		q := r.URL.Query().Get(column)
		if q == "" {
			continue
		}

		switch field.Type.Kind() {
		case reflect.String:
			f.SetString(q)
		case reflect.Bool:
			v, e := strconv.ParseBool(q)
			if e != nil {
				err = e
				glog.Error(err)
				return
			}
			f.SetBool(v)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			v, e := strconv.ParseInt(q, 10, 64)
			if e != nil {
				err = e
				glog.Error(err)
				return
			}
			f.SetInt(v)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			v, e := strconv.ParseUint(q, 10, 64)
			if e != nil {
				err = e
				glog.Error(err)
				return
			}
			f.SetUint(v)
		case reflect.Float32, reflect.Float64:
			v, e := strconv.ParseFloat(q, 64)
			if e != nil {
				err = e
				glog.Error(err)
				return
			}
			f.SetFloat(v)
		default:
			err = fmt.Errorf("unhandled field type: %v", field.Type.Kind())
			glog.Error(err)
			return
		}
	}
	return
}
