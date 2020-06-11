package helpers

import (
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

func TestSafeToObjectId(t *testing.T) {
	var input interface{} = bson.ObjectIdHex("57605d5dc3d5a2429db0bd09")
	assert.Equal(t, bson.ObjectIdHex("57605d5dc3d5a2429db0bd09"), SafeToObjectId(input))
}

func TestSafeToObjectId_Invalid(t *testing.T) {
	var input interface{} = "abc"
	assert.Panics(t, func() {
		SafeToObjectId(input)
	})
}

func TestSafeToDouble(t *testing.T) {
	var input interface{} = 123.45
	assert.Equal(t, 123.45, SafeToDouble(input))
}

func TestSafeToDouble_Invalid(t *testing.T) {
	var input interface{} = "abc"
	assert.Panics(t, func() {
		SafeToDouble(input)
	})
}

func TestSafeToString(t *testing.T) {
	var input interface{} = "abcdef"
	assert.Equal(t, "abcdef", SafeToString(input))
}

func TestSafeToString_Invalid(t *testing.T) {
	var input interface{}
	assert.Panics(t, func() {
		SafeToString(input)
	})
}

func TestSafeToBool(t *testing.T) {
	var input interface{} = true
	assert.Equal(t, true, SafeToBool(input))
}

func TestSafeToBool_Invalid(t *testing.T) {
	var input interface{} = "abc"
	assert.Panics(t, func() {
		SafeToBool(input)
	})
}

func TestSafeToDate(t *testing.T) {
	var input interface{} = ParseDate("2017-01-15T10:14:05+00:00")
	assert.Equal(t, ParseDate("2017-01-15T10:14:05+00:00"), SafeToDate(input))
}

func TestSafeToDate_Invalid(t *testing.T) {
	var input interface{} = "abc"
	assert.Panics(t, func() {
		SafeToDate(input)
	})
}

func TestSafeToInt(t *testing.T) {
	var input interface{} = 756
	assert.Equal(t, 756, SafeToInt(input))
}

func TestSafeToInt_Invalid(t *testing.T) {
	var input interface{} = "abc"
	assert.Panics(t, func() {
		SafeToInt(input)
	})
}

func TestSafeToInt32(t *testing.T) {
	var input interface{} = int32(756)
	assert.Equal(t, int32(756), SafeToInt32(input))
}

func TestSafeToInt32_Invalid(t *testing.T) {
	var input interface{} = "abc"
	assert.Panics(t, func() {
		SafeToInt32(input)
	})
}

func TestSafeToTimestamp(t *testing.T) {
	var input interface{} = bson.MongoTimestamp(14256987)
	assert.Equal(t, bson.MongoTimestamp(14256987), SafeToTimestamp(input))
}

func TestSafeToTimestamp_Invalid(t *testing.T) {
	var input interface{} = "abc"
	assert.Panics(t, func() {
		SafeToTimestamp(input)
	})
}

func TestSafeToLong(t *testing.T) {
	var input interface{} = int64(1427)
	assert.Equal(t, int64(1427), SafeToLong(input))
}

func TestSafeToLong_Invalid(t *testing.T) {
	var input interface{} = "abc"
	assert.Panics(t, func() {
		SafeToLong(input)
	})
}

func TestSafeToDecimal(t *testing.T) {
	var input interface{} = ParseDecimal("136.789")
	assert.Equal(t, ParseDecimal("136.789"), SafeToDecimal(input))
}

func TestSafeToDecimal_Invalid(t *testing.T) {
	var input interface{} = "abc"
	assert.Panics(t, func() {
		SafeToDecimal(input)
	})
}
