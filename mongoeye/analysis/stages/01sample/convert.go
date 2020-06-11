package sample

import (
	"gopkg.in/mgo.v2/bson"
	"reflect"
)

// RawToBsonChannel converts the channel of raw (binary) data from DB to channel of BSON documents.
func RawToBsonChannel(input interface{}) <-chan bson.M {
	if inputCh, ok := input.(chan []byte); ok {
		outCh := make(chan bson.M)

		go func() {
			for bin := range inputCh {
				out := bson.M{}
				bson.Unmarshal(bin, &out)
				outCh <- out
			}
			close(outCh)
		}()

		return outCh
	}

	panic("Invalid input. Expected 'chan []byte'. Given: " + reflect.TypeOf(input).String())
}

// BsonChannelToSlice reads BSON documents from channel into slice.
func BsonChannelToSlice(ch <-chan bson.M) []interface{} {
	s := []interface{}{}
	for i := range ch {
		s = append(s, i)
	}
	return s
}
