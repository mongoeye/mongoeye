package helpers

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetYAMLFieldName(t *testing.T) {
	type T struct {
		A bool
		B int    `yaml:"myb"`
		C string `yaml:"myc,omitempty"`
		D string `yaml:",omitempty" yaml:"yamlkey"`
		E int64  `yaml:",minsize"`
		F int64  `yaml:"myf,omitempty,minsize"`
	}

	assert.Panics(t, func() {
		GetYAMLFieldName(T{}, "A")
	})
	assert.Equal(t, "myb", GetYAMLFieldName(T{}, "B"))
	assert.Equal(t, "myc", GetYAMLFieldName(T{}, "C"))
	assert.Panics(t, func() {
		GetYAMLFieldName(T{}, "D")
	})
	assert.Panics(t, func() {
		GetYAMLFieldName(T{}, "E")
	})
	assert.Panics(t, func() {
		GetYAMLFieldName(T{}, "X")
	})
	assert.Equal(t, "myf", GetYAMLFieldName(T{}, "F"))
}

func TestMarshalToYAML(t *testing.T) {
	assert.Equal(t, "123\n", MarshalToYAML(123))
}
