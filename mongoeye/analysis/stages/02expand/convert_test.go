package expand

import (
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

func TestToValueChannel_ValueChannel(t *testing.T) {
	ch := make(chan Value, 10)
	valueCh := ToValueChannel(ch, 1, 1)

	ch <- Value{Name: "f1", Type: "string", Level: 0, Value: "abc", Length: 3}
	ch <- Value{Name: "f2", Type: "int", Level: 1, Value: 100}
	close(ch)

	var r Value
	var ok bool

	r, ok = <-valueCh
	assert.Equal(t, Value{Name: "f1", Type: "string", Level: 0, Value: "abc", Length: 3}, r)
	assert.Equal(t, true, ok)

	r, ok = <-valueCh
	assert.Equal(t, Value{Name: "f2", Type: "int", Level: 1, Value: 100}, r)
	assert.Equal(t, true, ok)

	_, ok = <-valueCh
	assert.Equal(t, false, ok)
}

func TestToValueChannel_ByteChannel(t *testing.T) {
	ch := make(chan []byte, 10)
	valueCh := ToValueChannel(ch, 1, 1)

	var raw []byte
	raw, _ = bson.Marshal(bson.M{BsonFieldName: "f1", BsonFieldType: "string", BsonLevel: 0, BsonValue: "abc", BsonLength: 3})
	ch <- raw

	raw, _ = bson.Marshal(bson.M{BsonFieldName: "f2", BsonFieldType: "int", BsonLevel: 1, BsonValue: 100})
	ch <- raw

	close(ch)

	var r Value
	var ok bool

	r, ok = <-valueCh
	assert.Equal(t, Value{Name: "f1", Type: "string", Level: 0, Value: "abc", Length: 3}, r)
	assert.Equal(t, true, ok)

	r, ok = <-valueCh
	assert.Equal(t, Value{Name: "f2", Type: "int", Level: 1, Value: 100}, r)
	assert.Equal(t, true, ok)

	_, ok = <-valueCh
	assert.Equal(t, false, ok)
}

func TestToValueChannel_InvalidType(t *testing.T) {
	ch := make(chan []int, 10)

	assert.Panics(t, func() {
		ToValueChannel(ch, 1, 1)
	})
}

func TestValueChannelToSlice(t *testing.T) {
	ch := make(chan Value, 10)

	v1 := Value{Name: "f1", Type: "string", Level: 0, Value: "abc", Length: 3}
	ch <- v1

	v2 := Value{Name: "f2", Type: "int", Level: 1, Value: 100}
	ch <- v2
	close(ch)

	assert.Equal(t, []interface{}{v1, v2}, ValueChannelToSlice(ch))
}
