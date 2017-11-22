package sampleInDB

import (
	"github.com/mongoeye/mongoeye/analysis/stages/01sample/tests"
	"github.com/mongoeye/mongoeye/tests"
	"testing"
)

func TestSampleInDB_Match(t *testing.T) {
	sampleTests.RunTestMatch(t, NewStage)
}

func TestSampleInDB_Project(t *testing.T) {
	sampleTests.RunTestProject(t, NewStage)
}

func TestSampleInDB_First(t *testing.T) {
	sampleTests.RunTestFirst(t, NewStage)
}

func TestSampleInDB_Last(t *testing.T) {
	sampleTests.RunTestLast(t, NewStage)
}

func TestSampleInDB_Random(t *testing.T) {
	if !tests.HasMongoDBSampleStageSupport() {
		t.Skip("Required $sample aggregation stage support.")
	}
	sampleTests.RunTestRandom(t, NewStage)
}

func TestSampleInDB_All(t *testing.T) {
	sampleTests.RunTestAll(t, NewStage)
}

func TestSampleInDB_InvalidSample(t *testing.T) {
	sampleTests.RunTestInvalidSample(t, NewStage)
}

func TestSampleInDB_InvalidLimitWithAllSample(t *testing.T) {
	sampleTests.RunTestInvalidLimitWithAllSample(t, NewStage)
}

func Benchmark_SampleFirst1000(b *testing.B) {
	sampleTests.RunBenchmarkSampleFirst1000(b, NewStage)
}

func Benchmark_SampleLast1000(b *testing.B) {
	sampleTests.RunBenchmarkSampleLast1000(b, NewStage)
}

func Benchmark_SampleRandom1000(b *testing.B) {
	sampleTests.RunBenchmarkSampleRandom1000(b, NewStage)
}

func Benchmark_SampleAll(b *testing.B) {
	sampleTests.RunBenchmarkSampleAll(b, NewStage)
}
