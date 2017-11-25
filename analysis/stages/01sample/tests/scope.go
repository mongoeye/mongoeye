package sampleTests

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/01sample"
	"github.com/mongoeye/mongoeye/tests/analysis"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

// RunTestFirst tests sample method = first.
func RunTestFirst(t *testing.T, sampleFactory sample.StageFactory) {
	c := setup()
	defer tearDown(c)

	options := sample.Options{
		Method: sample.FirstNDocuments,
		Limit:  2,
	}

	expected := []interface{}{
		bson.M{
			"_id":  bson.ObjectIdHex("58ed0817344c64f7fca5847a"),
			"key1": 123,
			"key2": "Abc",
		},
		bson.M{
			"_id":  bson.ObjectIdHex("58ed0817344c64f7fca5847b"),
			"key1": 456,
			"key2": "Cde",
		},
	}

	testStage(t, c, sampleFactory(&options), expected)
}

// RunTestLast tests sample method = last.
func RunTestLast(t *testing.T, sampleFactory sample.StageFactory) {
	c := setup()
	defer tearDown(c)

	options := sample.Options{
		Method: sample.LastNDocuments,
		Limit:  2,
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

// RunTestRandom tests sample method = random.
func RunTestRandom(t *testing.T, sampleFactory sample.StageFactory) {
	c := setup()
	defer tearDown(c)

	options := sample.Options{
		Method: sample.RandomNDocuments,
		Limit:  2,
	}

	out := analysisTests.RunStages(c, time.UTC, []*analysis.Stage{
		sampleFactory(&options),
	})

	results := sample.BsonChannelToSlice(sample.RawToBsonChannel(out))

	assert.Equal(t, 2, len(results))
}

// RunTestAll tests sample method = all.
func RunTestAll(t *testing.T, sampleFactory sample.StageFactory) {
	c := setup()
	defer tearDown(c)

	options := sample.Options{
		Method: sample.AllDocuments,
	}

	expected := []interface{}{
		bson.M{
			"_id":  bson.ObjectIdHex("58ed0817344c64f7fca5847a"),
			"key1": 123,
			"key2": "Abc",
		},
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

// RunTestInvalidSample tests invalid sample method.
func RunTestInvalidSample(t *testing.T, sampleFactory sample.StageFactory) {
	c := setup()
	defer tearDown(c)

	options := sample.Options{
		Method: 123,
	}

	assert.Panics(t, func() {
		testStage(t, c, sampleFactory(&options), nil)
	})
}

// RunTestInvalidLimitWithAllSample tests limit together with all sample.
func RunTestInvalidLimitWithAllSample(t *testing.T, sampleFactory sample.StageFactory) {
	c := setup()
	defer tearDown(c)

	options := sample.Options{
		Method: sample.AllDocuments,
		Limit:  1,
	}

	assert.Panics(t, func() {
		testStage(t, c, sampleFactory(&options), nil)
	})
}
