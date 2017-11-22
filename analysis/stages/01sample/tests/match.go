package sampleTests

import (
	"github.com/mongoeye/mongoeye/analysis/stages/01sample"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

// RunTestMatch tests sample stage with match option.
func RunTestMatch(t *testing.T, sampleFactory sample.StageFactory) {
	c := setup()
	defer tearDown(c)

	options := sample.Options{
		Method: sample.AllDocuments,
		Match: bson.M{
			"key1": bson.M{"$gt": 123},
		},
	}

	expected := []interface{}{
		bson.M{
			"_id":  bson.ObjectIdHex("58ed0817344c64f7fca5847b"),
			"key1": 456,
			"key2": "Cde",
		},
		bson.M{
			"_id":  bson.ObjectIdHex("58ed0817344c64f7fca5847c"),
			"key1": 789,
			"key2": "Xyz",
		},
	}

	testStage(t, c, sampleFactory(&options), expected)
}
