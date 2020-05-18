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
	city     string `db:"city"`
}

func TestDBMapColumn(t *testing.T) {
	isMale := false
	r := dbRecord{
		Name:     "Tom",      // shoud me mapped
		IsMale:   &isMale,    // shoud me mapped
		Age:      0,          // empty value should be ignored
		Nickname: "",         // empty value should be ignored
		IsVIP:    nil,        // empty value should be ignored
		Country:  "China",    // field without `db` tag should be ignored
		city:     "Shanghai", // private field should be ignored
	}

	table := MapColumn(r)

	assert.Equal(t, 2, len(table))
	assert.Equal(t, table["name"], "Tom")
	assert.Equal(t, table["is_male"], &isMale)
}
