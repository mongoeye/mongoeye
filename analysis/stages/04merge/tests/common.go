// Package mergeTests contains common tests for merge stage.
package mergeTests

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/01sample"
	"github.com/mongoeye/mongoeye/analysis/stages/01sample/sampleInDB"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand/expandInDBDepth"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand/expandLocally"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/analysis/stages/03group/groupInDB"
	"github.com/mongoeye/mongoeye/analysis/stages/03group/groupLocally"
	"github.com/mongoeye/mongoeye/analysis/stages/04merge"
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/mongoeye/mongoeye/tests"
	"github.com/mongoeye/mongoeye/tests/analysis"
	"gopkg.in/mgo.v2"
	"runtime"
	"testing"
	"time"
)

var testMergeOptions = &merge.Options{}

var sampleInDbStage = sampleInDB.NewStage(&sample.Options{})
var expandInDBStage *analysis.Stage
var expandLocallyStage *analysis.Stage
var groupInDBStage *analysis.Stage
var groupLocallyStage *analysis.Stage

func init() {
	expandOptions := &expand.Options{
		StringMaxLength:   100,
		ArrayMaxLength:    10,
		MaxDepth:          5,
		StoreValue:        true,
		StoreStringLength: true,
		StoreArrayLength:  true,
		StoreObjectLength: true,
	}
	expandInDBStage = expandInDBDepth.NewStage(expandOptions)
	expandLocallyStage = expandLocally.NewStage(expandOptions)

	groupOptions := &group.Options{
		ProcessObjectIdAsDate: false,
		StoreMinMaxAvgValue:   true,
		StoreMinMaxAvgLength:  true,
		StoreCountOfUnique:    true,
		StoreWeekdayHistogram: true,
		StoreHourHistogram:    true,
		StoreTopNValues:       20,
		StoreBottomNValues:    20,
		ValueHistogramMaxRes:  1000,
		LengthHistogramMaxRes: 100,
	}
	groupInDBStage = groupInDB.NewStage(groupOptions)
	groupLocallyStage = groupLocally.NewStage(groupOptions)
}

func testStage(t *testing.T, c *mgo.Collection, location *time.Location, mergeStage *analysis.Stage, expected []interface{}) []interface{} {
	numCpu := runtime.NumCPU()
	runtime.GOMAXPROCS(numCpu)

	var expandStage *analysis.Stage
	if mergeStage.PipelineFactory != nil {
		expandStage = expandInDBStage
	} else {
		expandStage = expandLocallyStage
	}

	var groupStage *analysis.Stage
	if mergeStage.PipelineFactory != nil {
		groupStage = groupInDBStage
	} else {
		groupStage = groupLocallyStage
	}

	out := analysisTests.RunStages(c, location, []*analysis.Stage{
		sampleInDbStage,
		expandStage,
		groupStage,
		mergeStage,
	})

	results := merge.FieldChannelToSlice(merge.ToFieldChannel(out, location, numCpu, 100))

	// Convert []Field -> []interface, for tests.AssertEqualSet function
	results2 := []interface{}{}
	for _, i := range results {
		results2 = append(results2, i)
	}

	tests.AssertEqualSet(t, expected, results2)

	return results2
}

func benchmarkStage(b *testing.B, c *mgo.Collection, location *time.Location, mergeStage *analysis.Stage, loadFields bool) {
	numCpu := runtime.NumCPU()
	runtime.GOMAXPROCS(numCpu)

	var expandStage *analysis.Stage
	if mergeStage.PipelineFactory != nil {
		expandStage = expandInDBStage
	} else {
		expandStage = expandLocallyStage
	}

	var groupStage *analysis.Stage
	if mergeStage.PipelineFactory != nil {
		groupStage = groupInDBStage
	} else {
		groupStage = groupLocallyStage
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		out := analysisTests.RunStages(c, location, []*analysis.Stage{
			sampleInDbStage,
			expandStage,
			groupStage,
			mergeStage,
		})

		if loadFields {
			s := merge.FieldChannelToSlice(merge.ToFieldChannel(out, location, numCpu, 100))
			b.Logf("Count: %d", len(s))
		} else {
			if ch, ok := out.(chan analysis.Field); ok {
				count := 0
				for range ch {
					count++
				}
				b.Logf("Count: %d", count)
			} else {
				b.Logf("Count: %d", helpers.ReadChannelToNull(out))
			}
		}
	}
}

func setup() *mgo.Collection {
	c := tests.CreateTestCollection(tests.TestDbSession)
	return c
}

func tearDown(c *mgo.Collection) {
	tests.DropTestCollection(c)
}
