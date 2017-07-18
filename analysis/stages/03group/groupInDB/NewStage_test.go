package groupInDB

import (
	"github.com/mongoeye/mongoeye/analysis/stages/03group/tests"
	"github.com/mongoeye/mongoeye/tests"
	"testing"
)

func TestGroupInDBBase(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestMinimal(t, NewStage)
}

func TestGroupInDBDifferentTypesInField(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestDifferentTypesInField(t, NewStage)
}

func TestGroupInDBValueMinMaxAvg(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestValueMinMaxAvg(t, NewStage)
}

func TestGroupInDBValueTopValues(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestValueTopValues(t, NewStage)
}

func TestGroupInDBValueTopValuesNGreaterThanNumberOfValues(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestValueTopValuesNGreaterThanNumberOfValues(t, NewStage)
}

func TestGroupInDBValueBottomValuesNGreaterThanNumberOfValues(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestValueBottomValuesNGreaterThanNumberOfValues(t, NewStage)
}

func TestGroupInDBValueBottomValues(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestValueBottomValues(t, NewStage)
}

func TestGroupInDBtCountOfUniqueValues(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestCountOfUniqueValues(t, NewStage)
}

func TestGroupInDBStringLengthMinMaxAvg(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestStringLengthMinMaxAvg(t, NewStage)
}

func TestGroupInDBArrayLengthMinMaxAvg(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestArrayLengthMinMaxAvg(t, NewStage)
}

func TestGroupInDBObjectLengthMinMaxAvg(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestObjectLengthMinMaxAvg(t, NewStage)
}

func TestGroupInDBValueHistogram(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestValueHistogram(t, NewStage)
}

func TestGroupInDBValueHistogramMaxRes(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestValueHistogramMaxRes(t, NewStage)
}

func TestGroupInDBLengthsHistogram(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestLengthHistogram(t, NewStage)
}

func TestGroupInDBLengthHistogramMaxRes(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestLengthHistogramMaxRes(t, NewStage)
}

func TestGroupInDBDateWeekdayHistogram(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestWeekdayHistogram(t, NewStage)
}

func TestGroupInDBDateHourHistogram(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestHourHistogram(t, NewStage)
}

func TestGroupInDBObjectIdAsDate(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestObjectIdAsDate(t, NewStage)
}

func TestGroupInDBDateStatsTimezone(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	groupTests.RunTestDateStatsTimezone(t, NewStage)
}

func BenchmarkGroupInDBMin(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	groupTests.RunBenchmarkStageMin(b, NewStage)
}

func BenchmarkGroupInDBMinMaxAvgValue(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	groupTests.RunBenchmarkStageMinMaxAvgValue(b, NewStage)
}

func BenchmarkGroupInDBMinMaxAvgLength(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	groupTests.RunBenchmarkStageMinMaxAvgLength(b, NewStage)
}

func BenchmarkGroupInDBCountOfUnique(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	groupTests.RunBenchmarkStageCountOfUnique(b, NewStage)
}

func BenchmarkGroupInDBMostFrequent(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	groupTests.RunBenchmarkStageMostFrequent(b, NewStage)
}

func BenchmarkGroupInDBLeastFrequent(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	groupTests.RunBenchmarkStageLeastFrequent(b, NewStage)
}

func BenchmarkGroupInDBValuesHistogram(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	groupTests.RunBenchmarkStageValuesHistogram(b, NewStage)
}

func BenchmarkGroupInDBLengthsHistogram(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	groupTests.RunBenchmarkStageLengthsHistogram(b, NewStage)
}

func BenchmarkGroupInDBDateWeekdayHistogram(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	groupTests.RunBenchmarkStageDateWeekdayHistogram(b, NewStage)
}

func BenchmarkGroupInDBDateHourHistogram(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	groupTests.RunBenchmarkStageDateHourHistogram(b, NewStage)
}

func BenchmarkGroupInDBObjectIdAsDate(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	groupTests.RunBenchmarkStageObjectIdAsDate(b, NewStage)
}

func BenchmarkGroupInDBObjectIdAsDateHistograms(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	groupTests.RunBenchmarkStageObjectIdAsDateHistograms(b, NewStage)
}

func BenchmarkGroupInDBFull(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	groupTests.RunBenchmarkStageFull(b, NewStage)
}
