package mergeLocally

import (
	"github.com/mongoeye/mongoeye/analysis/stages/04merge/tests"
	"testing"
)

func TestMergeLocallyAllTypes(t *testing.T) {
	mergeTests.RunTestAllTypes(t, NewStage)
}

func BenchmarkMergeLocallyFull(b *testing.B) {
	mergeTests.RunBenchmarkStageFull(b, NewStage)
}
