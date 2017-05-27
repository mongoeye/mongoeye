package expandInDBSeq

import (
	"github.com/mongoeye/mongoeye/analysis/stages/02expand/tests"
	"github.com/mongoeye/mongoeye/tests"
	"testing"
)

func TestExpandInDBSeqArrayFieldMinimal(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestArrayFieldMinimal(t, NewStage)
}

func TestExpandInDBSeqArrayFieldLength(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestArrayFieldLength(t, NewStage)
}

func TestExpandInDBSeqArrayFieldValue(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestArrayFieldValue(t, NewStage)
}

func TestExpandInDBSeqArrayFieldArrayMaxLength(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestArrayFieldArrayMaxLength(t, NewStage)
}

func TestExpandInDBSeqArrayFieldMaxDepth(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestArrayFieldMaxDepth(t, NewStage)
}

func TestExpandInDBSeqObjectFieldMinimal(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestObjectFieldMinimal(t, NewStage)
}

func TestExpandInDBSeqObjectFieldLength(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestObjectFieldLength(t, NewStage)
}

func TestExpandInDBSeqObjectFieldMaxDepth(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestObjectFieldMaxDepth(t, NewStage)
}

func TestExpandInDBSeqObjectFieldValue(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestObjectFieldValue(t, NewStage)
}

func TestExpandInDBSeqScalarTypesMinimal(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestScalarTypesMinimal(t, NewStage)
}

func TestExpandInDBSeqScalarTypesValue(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestScalarTypesValue(t, NewStage)
}

func TestExpandInDBSeqStringFieldMinimal(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestStringFieldMinimal(t, NewStage)
}

func TestExpandInDBSeqStringFieldValue(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestStringFieldValue(t, NewStage)
}

func TestExpandInDBSeqStringFieldLength(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestStringFieldLength(t, NewStage)
}

func TestExpandLocallyStringFieldMaxLength(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestStringFieldMaxLength(t, NewStage)
}

func TestExpandInDBSeqStringFieldAll(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestStringFieldAll(t, NewStage)
}

func BenchmarkExpandInDBSeqDepth0MinFull(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkDepth0Min(b, NewStage)
}

func BenchmarkExpandInDBSeqDepth0Full(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkDepth0Full(b, NewStage)
}

func BenchmarkExpandInDBSeqDepth5Full(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkDepth5Full(b, NewStage)
}

func BenchmarkExpandInDBSeqDoubleField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkDoubleField(b, NewStage)
}

func BenchmarkExpandInDBSeqStringField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkStringField(b, NewStage)
}

func BenchmarkExpandInDBSeqBinDataField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkBinDataField(b, NewStage)
}

func BenchmarkExpandInDBSeqUndefinedField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkUndefinedField(b, NewStage)
}

func BenchmarkExpandInDBSeqBoolField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkBoolField(b, NewStage)
}

func BenchmarkExpandInDBSeqDateField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkDateField(b, NewStage)
}

func BenchmarkExpandInDBSeqNullField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkNullField(b, NewStage)
}

func BenchmarkExpandInDBSeqObjectIdField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkObjectIdField(b, NewStage)
}

func BenchmarkExpandInDBSeqRegexField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkRegexField(b, NewStage)
}

func BenchmarkExpandInDBSeqDbPointerField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkDbPointerField(b, NewStage)
}

func BenchmarkExpandInDBSeqJavascriptField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkJavascriptField(b, NewStage)
}

func BenchmarkExpandInDBSeqSymbolField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkSymbolField(b, NewStage)
}

func BenchmarkExpandInDBSeqJavascriptWithField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkJavascriptWithField(b, NewStage)
}

func BenchmarkExpandInDBSeqIntField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkIntField(b, NewStage)
}

func BenchmarkExpandInDBSeqTimestampField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkTimestampField(b, NewStage)
}

func BenchmarkExpandInDBSeqLongField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkLongField(b, NewStage)
}

func BenchmarkExpandInDBSeqDecimalField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkDecimalField(b, NewStage)
}

func BenchmarkExpandInDBSeqMinKeyField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkMinKeyField(b, NewStage)
}

func BenchmarkExpandInDBSeqMaxKeyField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkMaxKeyField(b, NewStage)
}

func BenchmarkExpandInDBSeqObjectField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkObjectField(b, NewStage)
}

func BenchmarkExpandInDBSeqArrayField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkArrayField(b, NewStage)
}
