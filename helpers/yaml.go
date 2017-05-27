package helpers

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"reflect"
	"strings"
)

// GetYAMLFieldName gets yaml tag name from struct field.
func GetYAMLFieldName(t interface{}, name string) string {
	r := reflect.TypeOf(t)
	field, ok := r.FieldByName(name)
	if !ok {
		panic(fmt.Sprintf("Tag for field %s was not found in struct %s", name, r.String()))
	}

	tag, ok := field.Tag.Lookup("yaml")
	if !ok {
		panic(fmt.Sprintf("YAML tag for field %s was not found in struct %s", name, r.String()))
	}

	tag = strings.Split(tag, ",")[0]

	if len(tag) == 0 {
		panic(fmt.Sprintf("YAML tag for field %s in struct %s is empty", name, r.String()))
	}

	return tag
}

// MarshalToYAML or panic.
func MarshalToYAML(data interface{}) string {
	j, err := yaml.Marshal(data)
	if err != nil {
		panic(err)
	}

	return string(j)
}
