package helpers

import (
	"fmt"
	"github.com/deckarep/golang-set"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

func TestHashData_NoCollisions(t *testing.T) {
	data := []interface{}{
		map[string]interface{}{
			"key": "value",
		},
		map[string]interface{}{
			"key1": "value",
		},
		map[string]interface{}{
			"key": "value1",
		},
		"string",
		[]string{"string"},
		[]interface{}{"string"},
		"String",
		"Šašo",
		123,
		ParseDecimal("123"),
		123.456,
		bson.M{"k1": "v1", "k2": "v2"},
		bson.M{"k1": "v2", "k2": "v1"},
		bson.M{"key": "value"},
		bson.M{"key": "value1"},
		bson.M{"key1": "value"},
		bson.M{
			"key1": bson.M{
				"int": 456,
				"nested": bson.M{
					"text": "abc",
				},
			},
		},
		bson.M{
			"key1": bson.M{
				"text": "abc",
				"nested": bson.M{
					"int": 456,
				},
			},
		},
		struct {
			Key   string
			Field string
		}{Key: "key", Field: "value"},
	}

	set := mapset.NewSet()

	for _, item := range data {
		hash := HashData(item)
		if set.Contains(hash) {
			assert.Fail(t, "Hash collision. Value: "+fmt.Sprint(item))
		}
		set.Add(hash)
	}
}

func TestHashData_RegardlessOrder(t *testing.T) {
	assert.Equal(t, HashData(bson.M{"k1": "v1", "k2": "v2"}), HashData(bson.M{"k2": "v2", "k1": "v1"}))
	assert.Equal(t, HashData(map[string]interface{}{"k1": "v1", "k2": "v2"}), HashData(map[string]interface{}{"k2": "v2", "k1": "v1"}))
	assert.Equal(t, HashData([]interface{}{1, 2}), HashData([]interface{}{2, 1}))
}

func TestHashData_Pointer(t *testing.T) {
	type testStructure struct{ Str string }
	assert.NotEqual(t, HashData(&testStructure{Str: "abc"}), HashData(&testStructure{Str: "xyz"}))
}

func TestHashData_Nil(t *testing.T) {
	assert.NotPanics(t, func() {
		HashData((*string)(nil))
	})
}
