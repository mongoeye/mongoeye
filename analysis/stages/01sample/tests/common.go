// Package sampleTests contains common tests for sample stage.
package sampleTests

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/01sample"
	"github.com/mongoeye/mongoeye/tests"
	"github.com/mongoeye/mongoeye/tests/analysis"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"runtime"
	"testing"
	"time"
)

func setup() *mgo.Collection {
	c := tests.CreateTestCollection(tests.TestDbSession)

	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58ed0817344c64f7fca5847a"),
		"key1": 123,
		"key2": "Abc",
	})
	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58ed0817344c64f7fca5847b"),
		"key1": 456,
		"key2": "Cde",
	})
	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58ed0817344c64f7fca5847c"),
		"key1": 789,
		"key2": "Xyz",
	})

	return c
}

func tearDown(c *mgo.Collection) {
	tests.DropTestCollection(c)
}

func testStage(t *testing.T, c *mgo.Collection, sampleStage *analysis.Stage, expected []interface{}) []interface{} {
	numCpu := runtime.NumCPU()
	runtime.GOMAXPROCS(numCpu)

	out := analysisTests.RunStages(c, time.UTC, []*analysis.Stage{
		sampleStage,
	})

	results := sample.BsonChannelToSlice(sample.RawToBsonChannel(out))

	tests.AssertEqualSet(t, expected, results)

	return results
}

func benchmarkStage(b *testing.B, c *mgo.Collection, sampleStage *analysis.Stage, loadResults bool) {
	numCpu := runtime.NumCPU()
	runtime.GOMAXPROCS(numCpu)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		out := analysisTests.RunStages(c, time.UTC, []*analysis.Stage{
			sampleStage,
		})

		if loadResults {
			sample.BsonChannelToSlice(sample.RawToBsonChannel(out))
			//b.Logf("Count: %d", len(s))
		} else {
			//b.Logf("Count: %d", helpers.ReadChannelToNull(out))
		}
	}
}
