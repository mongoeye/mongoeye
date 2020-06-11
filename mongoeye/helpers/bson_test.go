package helpers

import (
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

func TestDataToString_String(t *testing.T) {
	assert.Equal(t, "\"šašo\"", DataToString("šašo"))
	assert.Equal(t, "123", DataToString(123))
	assert.Equal(t, "\"double(123.45000)\"", DataToString(123.45))
	assert.Equal(t, "\"decimal(789.123)\"", DataToString(ParseDecimal("789.123")))
	assert.Equal(t, "\"2006-01-02 15:04:05 -0700 -0700\"", DataToString(ParseDate("2006-01-02T15:04:05-07:00")))
	assert.Equal(t, "{\n\t\"FieldA\": \"fieldA\",\n\t\"FieldB\": 123,\n\t\"FieldC\": \"double(456.78900)\"\n}", DataToString(struct {
		FieldA string
		FieldB int
		FieldC float64
	}{FieldA: "fieldA", FieldB: 123, FieldC: 456.789}))
	assert.Equal(t, "[\n\t\"abc\",\n\t456\n]", DataToString([]interface{}{"abc", 456}))
}

func TestGetBSONFieldName(t *testing.T) {
	type T struct {
		A bool
		B int    `bson:"myb"`
		C string `bson:"myc,omitempty"`
		D string `bson:",omitempty" json:"jsonkey"`
		E int64  `bson:",minsize"`
		F int64  `bson:"myf,omitempty,minsize"`
	}

	assert.Panics(t, func() {
		GetBSONFieldName(T{}, "A")
	})
	assert.Equal(t, "myb", GetBSONFieldName(T{}, "B"))
	assert.Equal(t, "myc", GetBSONFieldName(T{}, "C"))
	assert.Panics(t, func() {
		GetBSONFieldName(T{}, "D")
	})
	assert.Panics(t, func() {
		GetBSONFieldName(T{}, "E")
	})
	assert.Panics(t, func() {
		GetBSONFieldName(T{}, "X")
	})
	assert.Equal(t, "myf", GetBSONFieldName(T{}, "F"))
}

func Test_replaceNonStringWithString(t *testing.T) {
	type testStructure struct{ Abc interface{} }

	assert.Equal(t, nil, replaceNonStringWithString(nil))
	assert.Equal(t, (*string)(nil), replaceNonStringWithString((*string)(nil)))
	assert.Equal(t, "decimal(123.45)", replaceNonStringWithString(ParseDecimal("123.45")))
	assert.Equal(t, "2006-01-01 10:00:00 +0000 UTC", replaceNonStringWithString(ParseDate("2006-01-01T10:00:00+00:00").In(time.UTC)))
	assert.Equal(t, "float(12.34000)", replaceNonStringWithString(float32(12.34)))
	assert.Equal(t, "double(12.34000)", replaceNonStringWithString(float64(12.34)))
	assert.Equal(t, map[string]interface{}{"Abc": "cde"}, replaceNonStringWithString(testStructure{Abc: "cde"}))
	assert.Equal(t, map[string]interface{}{"Abc": "decimal(123.45)"}, replaceNonStringWithString(testStructure{Abc: ParseDecimal("123.45")}))
	assert.Equal(t, map[string]interface{}{"Abc": "xyz"}, replaceNonStringWithString(&testStructure{Abc: "xyz"}))
	assert.Equal(t, []interface{}{1, 2, "decimal(123.45)"}, replaceNonStringWithString([]interface{}{1, 2, ParseDecimal("123.45")}))
	assert.Equal(t, map[string]interface{}{"Abc": "decimal(123.45)"}, replaceNonStringWithString(map[string]interface{}{"Abc": ParseDecimal("123.45")}))
	assert.Equal(t, bson.M{"Abc": "decimal(123.45)"}, replaceNonStringWithString(bson.M{"Abc": ParseDecimal("123.45")}))
}
