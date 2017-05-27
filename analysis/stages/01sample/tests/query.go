package sampleTests

import (
	"github.com/mongoeye/mongoeye/analysis/stages/01sample"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

// RunTestQuery tests sample stage with query option.
func RunTestQuery(t *testing.T, sampleFactory sample.StageFactory) {
	c := setup()
	defer tearDown(c)

	options := sample.Options{
		Scope: sample.All,
		Query: bson.M{
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
