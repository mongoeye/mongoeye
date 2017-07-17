package groupLocally

import (
	"github.com/mongoeye/mongoeye/analysis/stages/03group/tests"
	"testing"
)

func TestGroupLocallyBase(t *testing.T) {
	groupTests.RunTestMinimal(t, NewStage)
}

func TestGroupLocallyDifferentTypesInField(t *testing.T) {
	groupTests.RunTestDifferentTypesInField(t, NewStage)
}

func TestGroupLocallyValueMinMaxAvg(t *testing.T) {
	groupTests.RunTestValueMinMaxAvg(t, NewStage)
}

func TestGroupLocallyValueTopValues(t *testing.T) {
	groupTests.RunTestValueTopValues(t, NewStage)
}

func TestGroupLocallyValueTopValuesNGreaterThanNumberOfValues(t *testing.T) {
	groupTests.RunTestValueTopValuesNGreaterThanNumberOfValues(t, NewStage)
}

func TestGroupLocallyValueBottomValuesNGreaterThanNumberOfValues(t *testing.T) {
	groupTests.RunTestValueBottomValuesNGreaterThanNumberOfValues(t, NewStage)
}

func TestGroupLocallyValueBottomValues(t *testing.T) {
	groupTests.RunTestValueBottomValues(t, NewStage)
}

func TestGroupLocallytCountOfUniqueValues(t *testing.T) {
	groupTests.RunTestCountOfUniqueValues(t, NewStage)
}

func TestGroupLocallyStringLengthMinMaxAvg(t *testing.T) {
	groupTests.RunTestStringLengthMinMaxAvg(t, NewStage)
}

func TestGroupLocallyArrayLengthMinMaxAvg(t *testing.T) {
	groupTests.RunTestArrayLengthMinMaxAvg(t, NewStage)
}

func TestGroupLocallyObjectLengthMinMaxAvg(t *testing.T) {
	groupTests.RunTestObjectLengthMinMaxAvg(t, NewStage)
}

func TestGroupLocallyValueHistogram(t *testing.T) {
	groupTests.RunTestValueHistogram(t, NewStage)
}

func TestGroupLocallyValueHistogramMaxRes(t *testing.T) {
	groupTests.RunTestValueHistogramMaxRes(t, NewStage)
}

func TestGroupLocallyLengthsHistogram(t *testing.T) {
	groupTests.RunTestLengthHistogram(t, NewStage)
}

func TestGroupLocallyLengthHistogramMaxRes(t *testing.T) {
	groupTests.RunTestLengthHistogramMaxRes(t, NewStage)
}

func TestGroupLocallyDateWeekdayHistogram(t *testing.T) {
	groupTests.RunTestWeekdayHistogram(t, NewStage)
}

func TestGroupLocallyDateHourHistogram(t *testing.T) {
	groupTests.RunTestHourHistogram(t, NewStage)
}

func TestGroupLocallyObjectIdAsDate(t *testing.T) {
	groupTests.RunTestObjectIdAsDate(t, NewStage)
}

func TestGroupLocallyDateStatsTimezone(t *testing.T) {
	groupTests.RunTestDateStatsTimezone(t, NewStage)
}

func BenchmarkGroupLocallyMin(b *testing.B) {
	groupTests.RunBenchmarkStageMin(b, NewStage)
}

func BenchmarkGroupLocallyMinMaxAvgValue(b *testing.B) {
	groupTests.RunBenchmarkStageMinMaxAvgValue(b, NewStage)
}

func BenchmarkGroupLocallyMinMaxAvgLength(b *testing.B) {
	groupTests.RunBenchmarkStageMinMaxAvgLength(b, NewStage)
}

func BenchmarkGroupLocallyCountOfUnique(b *testing.B) {
	groupTests.RunBenchmarkStageCountOfUnique(b, NewStage)
}

func BenchmarkGroupLocallyMostFrequent(b *testing.B) {
	groupTests.RunBenchmarkStageMostFrequent(b, NewStage)
}

func BenchmarkGroupLocallyLeastFrequent(b *testing.B) {
	groupTests.RunBenchmarkStageLeastFrequent(b, NewStage)
}

func BenchmarkGroupLocallyValuesHistogram(b *testing.B) {
	groupTests.RunBenchmarkStageValuesHistogram(b, NewStage)
}

func BenchmarkGroupLocallyLengthsHistogram(b *testing.B) {
	groupTests.RunBenchmarkStageLengthsHistogram(b, NewStage)
}

func BenchmarkGroupLocallyDateWeekdayHistogram(b *testing.B) {
	groupTests.RunBenchmarkStageDateWeekdayHistogram(b, NewStage)
}

func BenchmarkGroupLocallyDateHourHistogram(b *testing.B) {
	groupTests.RunBenchmarkStageDateHourHistogram(b, NewStage)
}

func BenchmarkGroupLocallyObjectIdAsDate(b *testing.B) {
	groupTests.RunBenchmarkStageObjectIdAsDate(b, NewStage)
}

func BenchmarkGroupLocallyObjectIdAsDateHistograms(b *testing.B) {
	groupTests.RunBenchmarkStageObjectIdAsDateHistograms(b, NewStage)
}

func BenchmarkGroupLocallyFull(b *testing.B) {
	groupTests.RunBenchmarkStageFull(b, NewStage)
}
