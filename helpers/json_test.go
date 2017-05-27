package helpers

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetJSONFieldName(t *testing.T) {
	type T struct {
		A bool
		B int    `json:"myb"`
		C string `json:"myc,omitempty"`
		D string `json:",omitempty" json:"jsonkey"`
		E int64  `json:",minsize"`
		F int64  `json:"myf,omitempty,minsize"`
	}

	assert.Panics(t, func() {
		GetJSONFieldName(T{}, "A")
	})
	assert.Equal(t, "myb", GetJSONFieldName(T{}, "B"))
	assert.Equal(t, "myc", GetJSONFieldName(T{}, "C"))
	assert.Panics(t, func() {
		GetJSONFieldName(T{}, "D")
	})
	assert.Panics(t, func() {
		GetJSONFieldName(T{}, "E")
	})
	assert.Panics(t, func() {
		GetJSONFieldName(T{}, "X")
	})
	assert.Equal(t, "myf", GetJSONFieldName(T{}, "F"))
}

func TestMarshalToJSON(t *testing.T) {
	assert.Equal(t, "123", MarshalToJSON(123))
}
