package pq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type dbRecord struct {
	Name     string `db:"name"`
	IsMale   *bool  `db:"is_male"`
	Age      int    `db:"age"`
	Nickname string `db:"nickname"`
	IsVIP    *bool  `db:"is_vip"`
	Country  string
	Files    []string `db:"files"`
	city     string   `db:"city"`
	Avg      int      `db:"avg"`
}

func TestDBMapColumn(t *testing.T) {
	isMale := false
	r := dbRecord{
		Name:     "Tom",              // shoud me mapped
		IsMale:   &isMale,            // shoud me mapped
		Age:      0,                  // empty value should be ignored
		Nickname: "",                 // empty value should be ignored
		IsVIP:    nil,                // empty value should be ignored
		Country:  "China",            // field without `db` tag should be ignored
		city:     "Shanghai",         // private field should be ignored
		Files:    []string{"1", "2"}, // array
		Avg:      0,                  // kept empty value
	}

	opt := MapColumnOption{
		KeepEmptyValueColums: []string{"avg"},
	}
	table := MapColumnWithOption(r, opt)
	assert.Equal(t, 3, len(table))
	assert.Equal(t, table["name"], "Tom")
	assert.Equal(t, table["is_male"], &isMale)
	assert.Equal(t, table["avg"], 0)
}
