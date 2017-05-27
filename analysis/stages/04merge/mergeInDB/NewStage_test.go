package mergeInDB

import (
	"github.com/mongoeye/mongoeye/analysis/stages/04merge/tests"
	"github.com/mongoeye/mongoeye/tests"
	"testing"
)

func TestMergeInDBAllTypes(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	mergeTests.RunTestAllTypes(t, NewStage)
}

func BenchmarkMergeInDBFull(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	mergeTests.RunBenchmarkStageFull(b, NewStage)
}
