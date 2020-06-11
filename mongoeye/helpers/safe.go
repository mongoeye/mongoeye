package helpers

import (
	"gopkg.in/mgo.v2/bson"
	"reflect"
	"time"
)

// SafeToObjectId converts interface to ObjectId or fail.
func SafeToObjectId(value interface{}) bson.ObjectId {
	if t, ok := value.(bson.ObjectId); ok {
		return t
	}
	panic("Unexpected type. Given: " + reflect.TypeOf(value).String())
}

// SafeToDouble converts interface to float64 or fail.
func SafeToDouble(value interface{}) float64 {
	if t, ok := value.(float64); ok {
		return t
	}
	panic("Unexpected type. Given: " + reflect.TypeOf(value).String())
}

// SafeToString converts interface to string or fail.
func SafeToString(value interface{}) string {
	if t, ok := value.(string); ok {
		return t
	}
	panic("Unexpected type. Given: " + reflect.TypeOf(value).String())
}

// SafeToBool converts interface to bool or fail.
func SafeToBool(value interface{}) bool {
	if t, ok := value.(bool); ok {
		return t
	}
	panic("Unexpected type. Given: " + reflect.TypeOf(value).String())
}

// SafeToDate converts interface to time.Time or fail.
func SafeToDate(value interface{}) time.Time {
	if t, ok := value.(time.Time); ok {
		return t
	}
	panic("Unexpected type. Given: " + reflect.TypeOf(value).String())
}

// SafeToInt converts interface to int or fail.
func SafeToInt(value interface{}) int {
	if t, ok := value.(int); ok {
		return t
	}
	panic("Unexpected type. Given: " + reflect.TypeOf(value).String())
}

// SafeToInt32 converts interface to int32 or fail.
func SafeToInt32(value interface{}) int32 {
	if t, ok := value.(int32); ok {
		return t
	}
	panic("Unexpected type. Given: " + reflect.TypeOf(value).String())
}

// SafeToTimestamp converts interface to bson.MongoTimestamp or fail.
func SafeToTimestamp(value interface{}) bson.MongoTimestamp {
	if t, ok := value.(bson.MongoTimestamp); ok {
		return t
	}
	panic("Unexpected type. Given: " + reflect.TypeOf(value).String())
}

// SafeToLong converts interface to long or fail.
func SafeToLong(value interface{}) int64 {
	if t, ok := value.(int64); ok {
		return t
	}
	panic("Unexpected type. Given: " + reflect.TypeOf(value).String())
}

// SafeToDecimal converts interface to decimal or fail.
func SafeToDecimal(value interface{}) bson.Decimal128 {
	if t, ok := value.(bson.Decimal128); ok {
		return t
	}
	panic("Unexpected type. Given: " + reflect.TypeOf(value).String())
}
