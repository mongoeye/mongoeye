// Package groupTests contains common tests for group stage.
package groupTests

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/01sample"
	"github.com/mongoeye/mongoeye/analysis/stages/01sample/sampleInDB"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand/expandInDBDepth"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand/expandLocally"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/tests"
	"github.com/mongoeye/mongoeye/tests/analysis"
	"gopkg.in/mgo.v2"
	"runtime"
	"testing"
	"time"
)

var testGroupOptions = group.Options{
	ProcessObjectIdAsDate: false,
	StoreMinMaxAvgValue:   false,
	StoreMinMaxAvgLength:  false,
	StoreCountOfUnique:    false,
	StoreWeekdayHistogram: false,
	StoreHourHistogram:    false,
	StoreTopNValues:       0,
	StoreBottomNValues:    0,
	ValueHistogramMaxRes:  0,
	LengthHistogramMaxRes: 0,
}

var sampleInDbStage = sampleInDB.NewStage(&sample.Options{})
var expandInDBStage *analysis.Stage
var expandLocallyStage *analysis.Stage

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
}

func testStage(t *testing.T, c *mgo.Collection, location *time.Location, groupStage *analysis.Stage, expected []interface{}) []interface{} {
	numCpu := runtime.NumCPU()
	runtime.GOMAXPROCS(numCpu)

	var expandStage *analysis.Stage
	if groupStage.PipelineFactory != nil {
		expandStage = expandInDBStage
	} else {
		expandStage = expandLocallyStage
	}

	out := analysisTests.RunStages(c, location, []*analysis.Stage{
		sampleInDbStage,
		expandStage,
		groupStage,
	})

	results := group.ResultChannelToSlice(group.ToResultChannel(out, location, numCpu, 100))

	tests.AssertEqualSet(t, expected, results)

	return results
}

func benchmarkStage(b *testing.B, c *mgo.Collection, location *time.Location, groupStage *analysis.Stage, loadResults bool) {
	numCpu := runtime.NumCPU()
	runtime.GOMAXPROCS(numCpu)

	var expandStage *analysis.Stage
	if groupStage.PipelineFactory != nil {
		expandStage = expandInDBStage
	} else {
		expandStage = expandLocallyStage
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		out := analysisTests.RunStages(c, location, []*analysis.Stage{
			sampleInDbStage,
			expandStage,
			groupStage,
		})

		if loadResults {
			group.ResultChannelToSlice(group.ToResultChannel(out, location, numCpu, 100))
			//b.Logf("Count: %d", len(s))
		} else {
			if ch, ok := out.(chan group.Result); ok {
				count := 0
				for range ch {
					count++
				}
				//b.Logf("Count: %d", count)
			} else {
				//b.Logf("Count: %d", helpers.ReadChannelToNull(out))
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
