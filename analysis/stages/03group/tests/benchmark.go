package groupTests

import (
	"github.com/jinzhu/copier"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/tests"
	"testing"
	"time"
)

// RunBenchmarkStageMin - tests speed of group stage with minimal configuration.
func RunBenchmarkStageMin(b *testing.B, stageFactory group.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)

	benchmarkStage(b, c, time.UTC, stageFactory(&options), false)
}

// RunBenchmarkStageMinMaxAvgValue - tests speed of group stage with StoreMinMaxAvgValue option.
func RunBenchmarkStageMinMaxAvgValue(b *testing.B, stageFactory group.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreMinMaxAvgValue = true

	benchmarkStage(b, c, time.UTC, stageFactory(&options), false)
}

// RunBenchmarkStageMinMaxAvgLength - tests speed of group stage with StoreMinMaxAvgLength option.
func RunBenchmarkStageMinMaxAvgLength(b *testing.B, stageFactory group.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreMinMaxAvgLength = true

	benchmarkStage(b, c, time.UTC, stageFactory(&options), false)
}

// RunBenchmarkStageCountOfUnique - tests speed of group stage with StoreCountOfUnique option.
func RunBenchmarkStageCountOfUnique(b *testing.B, stageFactory group.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreCountOfUnique = true

	benchmarkStage(b, c, time.UTC, stageFactory(&options), false)
}

// RunBenchmarkStageTopNValues - tests speed of group stage with StoreTopNValues option.
func RunBenchmarkStageTopNValues(b *testing.B, stageFactory group.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreTopNValues = 20

	benchmarkStage(b, c, time.UTC, stageFactory(&options), false)
}

// RunBenchmarkStageBottomNValues - tests speed of group stage with StoreBottomNValues option.
func RunBenchmarkStageBottomNValues(b *testing.B, stageFactory group.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreBottomNValues = 20

	benchmarkStage(b, c, time.UTC, stageFactory(&options), false)
}

// RunBenchmarkStageValuesHistogram - tests speed of group stage with ValueHistogramMaxRes option.
func RunBenchmarkStageValuesHistogram(b *testing.B, stageFactory group.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.ValueHistogramMaxRes = 1000

	benchmarkStage(b, c, time.UTC, stageFactory(&options), false)
}

// RunBenchmarkStageLengthsHistogram - tests speed of group stage with LengthHistogramMaxRes option.
func RunBenchmarkStageLengthsHistogram(b *testing.B, stageFactory group.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.LengthHistogramMaxRes = 1000

	benchmarkStage(b, c, time.UTC, stageFactory(&options), false)
}

// RunBenchmarkStageDateWeekdayHistogram - tests speed of group stage with StoreWeekdayHistogram option.
func RunBenchmarkStageDateWeekdayHistogram(b *testing.B, stageFactory group.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreWeekdayHistogram = true

	benchmarkStage(b, c, time.UTC, stageFactory(&options), false)
}

// RunBenchmarkStageDateHourHistogram - tests speed of group stage with StoreHourHistogram option.
func RunBenchmarkStageDateHourHistogram(b *testing.B, stageFactory group.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreHourHistogram = true

	benchmarkStage(b, c, time.UTC, stageFactory(&options), false)
}

// RunBenchmarkStageObjectIdAsDate - tests speed of group stage with ProcessObjectIdAsDate option.
func RunBenchmarkStageObjectIdAsDate(b *testing.B, stageFactory group.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.ProcessObjectIdAsDate = true

	benchmarkStage(b, c, time.UTC, stageFactory(&options), false)
}

// RunBenchmarkStageObjectIdAsDateHistograms - tests speed of group stage with ProcessObjectIdAsDate, StoreHourHistogram, ValueHistogramMaxRes, option.
func RunBenchmarkStageObjectIdAsDateHistograms(b *testing.B, stageFactory group.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.ProcessObjectIdAsDate = true
	options.StoreWeekdayHistogram = true
	options.StoreHourHistogram = true
	options.ValueHistogramMaxRes = 1000

	benchmarkStage(b, c, time.UTC, stageFactory(&options), false)
}

// RunBenchmarkStageFull - tests speed of group stage with all options.
func RunBenchmarkStageFull(b *testing.B, stageFactory group.StageFactory) {
	b.StopTimer()

	c := tests.BenchmarkDbSession.DB(tests.BenchmarkDb).C(tests.BenchmarkCol)

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.ProcessObjectIdAsDate = true
	options.StoreMinMaxAvgValue = true
	options.StoreMinMaxAvgLength = true
	options.StoreCountOfUnique = true
	options.StoreWeekdayHistogram = true
	options.StoreHourHistogram = true
	options.StoreTopNValues = 20
	options.StoreBottomNValues = 20
	options.ValueHistogramMaxRes = 1000
	options.LengthHistogramMaxRes = 100

	benchmarkStage(b, c, time.UTC, stageFactory(&options), false)
}
