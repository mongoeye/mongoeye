package sampleTests

import (
	"github.com/mongoeye/mongoeye/analysis/stages/01sample"
	"github.com/mongoeye/mongoeye/tests"
	"testing"
)

// RunBenchmarkSampleFirst1000 tests speed of sampling first 1000 documents.
func RunBenchmarkSampleFirst1000(b *testing.B, sampleFactory sample.StageFactory) {
	b.StopTimer()

	c := tests.GetBenchmarkCol()

	options := sample.Options{
		Method: sample.FirstNDocuments,
		Limit:  1000,
	}

	benchmarkStage(b, c, sampleFactory(&options), true)
}

// RunBenchmarkSampleLast1000 tests speed of sampling last 1000 documents.
func RunBenchmarkSampleLast1000(b *testing.B, sampleFactory sample.StageFactory) {
	b.StopTimer()

	c := tests.GetBenchmarkCol()

	options := sample.Options{
		Method: sample.LastNDocuments,
		Limit:  1000,
	}

	benchmarkStage(b, c, sampleFactory(&options), true)
}

// RunBenchmarkSampleRandom1000 tests speed of sampling random 1000 documents.
func RunBenchmarkSampleRandom1000(b *testing.B, sampleFactory sample.StageFactory) {
	b.StopTimer()

	c := tests.GetBenchmarkCol()

	options := sample.Options{
		Method: sample.RandomNDocuments,
		Limit:  1000,
	}

	benchmarkStage(b, c, sampleFactory(&options), true)
}

// RunBenchmarkSampleAll tests speed of sampling all documents.
func RunBenchmarkSampleAll(b *testing.B, sampleFactory sample.StageFactory) {
	b.StopTimer()

	c := tests.GetBenchmarkCol()

	options := sample.Options{
		Method: sample.AllDocuments,
	}

	benchmarkStage(b, c, sampleFactory(&options), true)
}
