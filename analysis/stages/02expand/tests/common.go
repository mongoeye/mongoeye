// Package expandTests contains common tests for expand stage.
package expandTests

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/01sample"
	"github.com/mongoeye/mongoeye/analysis/stages/01sample/sampleInDB"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/tests"
	"github.com/mongoeye/mongoeye/tests/analysis"
	"gopkg.in/mgo.v2"
	"runtime"
	"testing"
	"time"
)

var testOptions = expand.Options{
	StringMaxLength:   100,
	ArrayMaxLength:    10,
	MaxDepth:          5,
	StoreValue:        false,
	StoreStringLength: false,
	StoreArrayLength:  false,
	StoreObjectLength: false,
}

var sampleInDbStage = sampleInDB.NewStage(&sample.Options{})

func testStage(t *testing.T, c *mgo.Collection, expandStage *analysis.Stage, expected []interface{}) []interface{} {
	numCpu := runtime.NumCPU()
	runtime.GOMAXPROCS(numCpu)

	out := analysisTests.RunStages(c, time.UTC, []*analysis.Stage{
		sampleInDbStage,
		expandStage,
	})

	results := expand.ValueChannelToSlice(expand.ToValueChannel(out, numCpu, 100))

	tests.AssertEqualSet(t, expected, results)

	return results
}

func benchmarkStage(b *testing.B, c *mgo.Collection, expandStage *analysis.Stage, loadResults bool) {
	numCpu := runtime.NumCPU()
	runtime.GOMAXPROCS(numCpu)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		out := analysisTests.RunStages(c, time.UTC, []*analysis.Stage{
			sampleInDbStage,
			expandStage,
		})

		if loadResults {
			expand.ValueChannelToSlice(expand.ToValueChannel(out, numCpu, 100))
			//b.Logf("Count: %d", len(s))
		} else {
			//b.Logf("Count: %d", helpers.ReadChannelToNull(out))
		}
	}
}
