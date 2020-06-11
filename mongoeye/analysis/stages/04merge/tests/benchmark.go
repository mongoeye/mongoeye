package mergeTests

import (
	"github.com/jinzhu/copier"
	"github.com/mongoeye/mongoeye/analysis/stages/04merge"
	"github.com/mongoeye/mongoeye/tests"
	"testing"
	"time"
)

// RunBenchmarkStageFull benchmarks speed of merge stage
func RunBenchmarkStageFull(b *testing.B, stageFactory merge.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := merge.Options{}
	copier.Copy(&options, testMergeOptions)

	benchmarkStage(b, c, time.UTC, stageFactory(&options), true)
}
