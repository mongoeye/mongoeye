package sample

import (
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

func TestRawToBsonChannel(t *testing.T) {
	doc1 := bson.M{
		"key1": "abc",
		"key2": 123,
	}
	doc2 := bson.M{
		"key1": "def",
		"key2": 456,
	}

	rawCh := make(chan []byte, 10)

	doc1Raw, _ := bson.Marshal(doc1)
	rawCh <- doc1Raw

	doc2Raw, _ := bson.Marshal(doc2)
	rawCh <- doc2Raw

	close(rawCh)

	bsonCh := RawToBsonChannel(rawCh)

	var doc bson.M
	var ok bool

	doc, ok = <-bsonCh
	assert.Equal(t, doc1, doc)
	assert.Equal(t, true, ok)

	doc, ok = <-bsonCh
	assert.Equal(t, doc2, doc)
	assert.Equal(t, true, ok)

	_, ok = <-bsonCh
	assert.Equal(t, false, ok)
}

func TestRawToBsonChannel_InvalidType(t *testing.T) {
	ch := make(chan int)
	assert.Panics(t, func() {
		RawToBsonChannel(ch)
	})
}

func TestBsonChannelToSlice(t *testing.T) {
	bsonCh := make(chan bson.M, 10)

	bsonCh <- bson.M{"key1": "value1"}
	bsonCh <- bson.M{"key2": "value2"}
	close(bsonCh)

	assert.Equal(t, []interface{}{bson.M{"key1": "value1"}, bson.M{"key2": "value2"}}, BsonChannelToSlice(bsonCh))
}
