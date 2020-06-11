package helpers

import (
	"github.com/fatih/structs"
	"gopkg.in/mgo.v2/bson"
	"hash/adler32"
	"reflect"
	"strconv"
)

// HashData hash structured data regardless of the order
func HashData(input interface{}) uint32 {
	return hashDataInLevel(input, 0)
}

func hashDataInLevel(input interface{}, level int) uint32 {
	if decimal, ok := input.(bson.Decimal128); ok {
		input = "decimal(" + strconv.FormatFloat(DecimalToDouble(decimal), 'G', -1, 64) + ")"
	}

	if reflect.Value(reflect.ValueOf(input)).Kind() == reflect.Ptr {
		v := reflect.Value(reflect.ValueOf(input))
		if v.IsNil() {
			return 10 + hashDataInLevel(nil, level+1)
		}
		return 10 + hashDataInLevel(v.Elem().Interface(), level+1)
	} else if reflect.Value(reflect.ValueOf(input)).Kind() == reflect.Struct {
		return 20 + hashDataInLevel(structs.Map(input), level+1)
	} else if m, ok := input.(map[string]interface{}); ok {
		var h uint32
		length := uint32(len(m))
		for key, value := range m {
			h += (100 + hashScalar(key) ^ hashDataInLevel(value, level+1)) / length
		}
		return h
	} else if m, ok := input.(bson.M); ok {
		var h uint32
		length := uint32(len(m))
		for key, value := range m {
			h += (200 + hashScalar(key) ^ hashDataInLevel(value, level+1)) / length
		}
		return h
	} else if a, ok := input.([]interface{}); ok {
		var h uint32
		length := uint32(len(a))
		for _, i := range a {
			h += 300 + hashDataInLevel(i, level+1)/length
		}
		return h
	}

	return 400 + hashScalar(input) + adler32.Checksum([]byte{byte(level)})
}

func hashScalar(input interface{}) uint32 {
	return adler32.Checksum([]byte(MarshalToJSON(input)))
}
