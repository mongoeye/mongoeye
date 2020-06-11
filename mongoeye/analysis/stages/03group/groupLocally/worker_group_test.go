package groupLocally

import (
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

func Test_storeMinMaxSum_ObjectId(t *testing.T) {
	a := &Accumulator{}

	storeMinMaxSum(a, "objectId", bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"))
	assert.Equal(t, bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"), a.MinValue)
	assert.Equal(t, bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"), a.MaxValue)

	storeMinMaxSum(a, "objectId", bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"))
	assert.Equal(t, bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"), a.MinValue)
	assert.Equal(t, bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"), a.MaxValue)

	storeMinMaxSum(a, "objectId", bson.ObjectIdHex("58e20d849d3ae7e1f8eac9b9"))
	assert.Equal(t, bson.ObjectIdHex("58e20d849d3ae7e1f8eac9b9"), a.MinValue)
	assert.Equal(t, bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"), a.MaxValue)
}

func Test_storeMinMaxSum_Double(t *testing.T) {
	a := &Accumulator{}

	storeMinMaxSum(a, "double", 12.34)
	assert.Equal(t, 12.34, a.MinValue)
	assert.Equal(t, 12.34, a.MaxValue)

	storeMinMaxSum(a, "double", 15.12)
	assert.Equal(t, 12.34, a.MinValue)
	assert.Equal(t, 15.12, a.MaxValue)

	storeMinMaxSum(a, "double", 11.54)
	assert.Equal(t, 11.54, a.MinValue)
	assert.Equal(t, 15.12, a.MaxValue)
}

func Test_storeMinMaxSum_String(t *testing.T) {
	a := &Accumulator{}

	storeMinMaxSum(a, "string", "bcd")
	assert.Equal(t, "bcd", a.MinValue)
	assert.Equal(t, "bcd", a.MaxValue)

	storeMinMaxSum(a, "string", "def")
	assert.Equal(t, "bcd", a.MinValue)
	assert.Equal(t, "def", a.MaxValue)

	storeMinMaxSum(a, "string", "abc")
	assert.Equal(t, "abc", a.MinValue)
	assert.Equal(t, "def", a.MaxValue)
}

func Test_storeMinMaxSum_Bool(t *testing.T) {
	a := &Accumulator{}

	storeMinMaxSum(a, "bool", true)
	assert.Equal(t, true, a.MinValue)
	assert.Equal(t, true, a.MaxValue)

	storeMinMaxSum(a, "bool", false)
	assert.Equal(t, false, a.MinValue)
	assert.Equal(t, true, a.MaxValue)

	storeMinMaxSum(a, "bool", false)
	assert.Equal(t, false, a.MinValue)
	assert.Equal(t, true, a.MaxValue)

	a = &Accumulator{}

	storeMinMaxSum(a, "bool", false)
	assert.Equal(t, false, a.MinValue)
	assert.Equal(t, false, a.MaxValue)

	storeMinMaxSum(a, "bool", true)
	assert.Equal(t, false, a.MinValue)
	assert.Equal(t, true, a.MaxValue)

	storeMinMaxSum(a, "bool", false)
	assert.Equal(t, false, a.MinValue)
	assert.Equal(t, true, a.MaxValue)
}

func Test_storeMinMaxSum_Date(t *testing.T) {
	a := &Accumulator{}

	storeMinMaxSum(a, "date", helpers.ParseDate("2006-01-01T01:01:00+00:00"))
	assert.Equal(t, helpers.ParseDate("2006-01-01T01:01:00+00:00"), a.MinValue)
	assert.Equal(t, helpers.ParseDate("2006-01-01T01:01:00+00:00"), a.MaxValue)

	storeMinMaxSum(a, "date", helpers.ParseDate("2004-01-01T01:01:00+00:00"))
	assert.Equal(t, helpers.ParseDate("2004-01-01T01:01:00+00:00"), a.MinValue)
	assert.Equal(t, helpers.ParseDate("2006-01-01T01:01:00+00:00"), a.MaxValue)

	storeMinMaxSum(a, "date", helpers.ParseDate("2010-01-01T01:01:00+00:00"))
	assert.Equal(t, helpers.ParseDate("2004-01-01T01:01:00+00:00"), a.MinValue)
	assert.Equal(t, helpers.ParseDate("2010-01-01T01:01:00+00:00"), a.MaxValue)
}

func Test_storeMinMaxSum_Int(t *testing.T) {
	a := &Accumulator{}

	storeMinMaxSum(a, "int", 10)
	assert.Equal(t, 10, a.MinValue)
	assert.Equal(t, 10, a.MaxValue)

	storeMinMaxSum(a, "int", 8)
	assert.Equal(t, 8, a.MinValue)
	assert.Equal(t, 10, a.MaxValue)

	storeMinMaxSum(a, "int", 12)
	assert.Equal(t, 8, a.MinValue)
	assert.Equal(t, 12, a.MaxValue)
}

func Test_storeMinMaxSum_Timestamp(t *testing.T) {
	a := &Accumulator{}

	storeMinMaxSum(a, "timestamp", bson.MongoTimestamp(2000))
	assert.Equal(t, bson.MongoTimestamp(2000), a.MinValue)
	assert.Equal(t, bson.MongoTimestamp(2000), a.MaxValue)

	storeMinMaxSum(a, "timestamp", bson.MongoTimestamp(1500))
	assert.Equal(t, bson.MongoTimestamp(1500), a.MinValue)
	assert.Equal(t, bson.MongoTimestamp(2000), a.MaxValue)

	storeMinMaxSum(a, "timestamp", bson.MongoTimestamp(5000))
	assert.Equal(t, bson.MongoTimestamp(1500), a.MinValue)
	assert.Equal(t, bson.MongoTimestamp(5000), a.MaxValue)
}

func Test_storeMinMaxSum_Long(t *testing.T) {
	a := &Accumulator{}

	storeMinMaxSum(a, "long", int64(2000))
	assert.Equal(t, int64(2000), a.MinValue)
	assert.Equal(t, int64(2000), a.MaxValue)

	storeMinMaxSum(a, "long", int64(1500))
	assert.Equal(t, int64(1500), a.MinValue)
	assert.Equal(t, int64(2000), a.MaxValue)

	storeMinMaxSum(a, "long", int64(5000))
	assert.Equal(t, int64(1500), a.MinValue)
	assert.Equal(t, int64(5000), a.MaxValue)
}

func Test_storeMinMaxSum_Decimal(t *testing.T) {
	a := &Accumulator{}

	storeMinMaxSum(a, "decimal", helpers.ParseDecimal("2000"))
	assert.Equal(t, helpers.ParseDecimal("2000"), a.MinValue)
	assert.Equal(t, helpers.ParseDecimal("2000"), a.MaxValue)

	storeMinMaxSum(a, "decimal", helpers.ParseDecimal("1500"))
	assert.Equal(t, helpers.ParseDecimal("1500"), a.MinValue)
	assert.Equal(t, helpers.ParseDecimal("2000"), a.MaxValue)

	storeMinMaxSum(a, "decimal", helpers.ParseDecimal("5000"))
	assert.Equal(t, helpers.ParseDecimal("1500"), a.MinValue)
	assert.Equal(t, helpers.ParseDecimal("5000"), a.MaxValue)
}

func Test_storeMinMaxSum_invalidType(t *testing.T) {
	assert.Panics(t, func() {
		storeMinMaxSum(&Accumulator{}, "invalid", nil)
	})
}
