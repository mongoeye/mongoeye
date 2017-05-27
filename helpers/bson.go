package helpers

import (
	"encoding/json"
	"fmt"
	"github.com/fatih/structs"
	"gopkg.in/mgo.v2/bson"
	"reflect"
	"strings"
	"time"
)

// DataToString formats structured data to string.
func DataToString(data interface{}) string {
	data = replaceNonStringWithString(data)
	b, _ := json.MarshalIndent(data, "", "\t")
	return string(b)
}

// Replace non-printable data structures with string representation (printable)
func replaceNonStringWithString(data interface{}) interface{} {
	if decimal, ok := data.(bson.Decimal128); ok {
		return fmt.Sprintf("decimal(%s)", decimal.String())
	} else if t, ok := data.(time.Time); ok {
		return t.String()
	} else if f, ok := data.(float32); ok {
		return fmt.Sprintf("float(%.5f)", f)
	} else if d, ok := data.(float64); ok {
		return fmt.Sprintf("double(%.5f)", d)
	} else if reflect.Value(reflect.ValueOf(data)).Kind() == reflect.Struct {
		return replaceNonStringWithString(structs.Map(data))
	} else if reflect.Value(reflect.ValueOf(data)).Kind() == reflect.Ptr {
		v := reflect.Value(reflect.ValueOf(data))
		if v.IsNil() {
			return data
		}
		return replaceNonStringWithString(v.Elem().Interface())
	} else if a, ok := data.([]interface{}); ok {
		for k, v := range a {
			a[k] = replaceNonStringWithString(v)
		}
		return a
	} else if m, ok := data.(map[string]interface{}); ok {
		for k, v := range m {
			m[k] = replaceNonStringWithString(v)
		}
		return m
	} else if m, ok := data.(bson.M); ok {
		for k, v := range m {
			m[k] = replaceNonStringWithString(v)
		}
		return m
	}

	return data
}

// PrintData prints structured data
func PrintData(data interface{}) {
	fmt.Println(DataToString(data))
}

// GetBSONFieldName gets bson tag name from struct field.
func GetBSONFieldName(t interface{}, name string) string {
	r := reflect.TypeOf(t)
	field, ok := r.FieldByName(name)
	if !ok {
		panic(fmt.Sprintf("Tag for field %s was not found in struct %s", name, r.String()))
	}

	tag, ok := field.Tag.Lookup("bson")
	if !ok {
		panic(fmt.Sprintf("BSON tag for field %s was not found in struct %s", name, r.String()))
	}

	tag = strings.Split(tag, ",")[0]

	if len(tag) == 0 {
		panic(fmt.Sprintf("BSON tag for field %s in struct %s is empty", name, r.String()))
	}

	return tag
}
