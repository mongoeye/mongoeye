package helpers

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// GetJSONFieldName gets json tag name from struct field.
func GetJSONFieldName(t interface{}, name string) string {
	r := reflect.TypeOf(t)
	field, ok := r.FieldByName(name)
	if !ok {
		panic(fmt.Sprintf("Tag for field %s was not found in struct %s", name, r.String()))
	}

	tag, ok := field.Tag.Lookup("json")
	if !ok {
		panic(fmt.Sprintf("JSON tag for field %s was not found in struct %s", name, r.String()))
	}

	tag = strings.Split(tag, ",")[0]

	if len(tag) == 0 {
		panic(fmt.Sprintf("JSON tag for field %s in struct %s is empty", name, r.String()))
	}

	return tag
}

// MarshalToJSON or panic.
func MarshalToJSON(data interface{}) string {
	j, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	return string(j)
}
