package sampleTests

import (
	"github.com/mongoeye/mongoeye/analysis/stages/01sample"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

// RunTestProject tests sample stage with project option.
func RunTestProject(t *testing.T, sampleFactory sample.StageFactory) {
	c := setup()
	defer tearDown(c)

	options := sample.Options{
		Scope: sample.All,
		Project: bson.M{
			"key1": 1,
		},
	}

	expected := []interface{}{
		bson.M{
			"_id":  bson.ObjectIdHex("58ed0817344c64f7fca5847a"),
			"key1": 123,
		},
		bson.M{
			"_id":  bson.ObjectIdHex("58ed0817344c64f7fca5847b"),
			"key1": 456,
		},
		bson.M{
			"_id":  bson.ObjectIdHex("58ed0817344c64f7fca5847c"),
			"key1": 789,
		},
	}

	testStage(t, c, sampleFactory(&options), expected)
}
