package expandInDBDepth

import (
	"github.com/mongoeye/mongoeye/analysis/stages/02expand/tests"
	"github.com/mongoeye/mongoeye/tests"
	"testing"
)

func TestExpandInDBDepthArrayFieldMinimal(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestArrayFieldMinimal(t, NewStage)
}

func TestExpandInDBDepthArrayFieldLength(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestArrayFieldLength(t, NewStage)
}

func TestExpandInDBDepthArrayFieldValue(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestArrayFieldValue(t, NewStage)
}

func TestExpandInDBDepthArrayFieldArrayMaxLength(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestArrayFieldArrayMaxLength(t, NewStage)
}

func TestExpandInDBDepthArrayFieldMaxDepth(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestArrayFieldMaxDepth(t, NewStage)
}

func TestExpandInDBDepthObjectFieldMinimal(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestObjectFieldMinimal(t, NewStage)
}

func TestExpandInDBDepthObjectFieldLength(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestObjectFieldLength(t, NewStage)
}

func TestExpandInDBDepthObjectFieldMaxDepth(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestObjectFieldMaxDepth(t, NewStage)
}

func TestExpandInDBDepthObjectFieldValue(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestObjectFieldValue(t, NewStage)
}

func TestExpandInDBDepthScalarTypesMinimal(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestScalarTypesMinimal(t, NewStage)
}

func TestExpandInDBDepthScalarTypesValue(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestScalarTypesValue(t, NewStage)
}

func TestExpandInDBDepthStringFieldMinimal(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestStringFieldMinimal(t, NewStage)
}

func TestExpandInDBDepthStringFieldValue(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestStringFieldValue(t, NewStage)
}

func TestExpandInDBDepthStringFieldLength(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestStringFieldLength(t, NewStage)
}

func TestExpandLocallyStringFieldMaxLength(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestStringFieldMaxLength(t, NewStage)
}

func TestExpandInDBDepthStringFieldAll(t *testing.T) {
	tests.SkipTIfNotSupportAggregationAlgorithm(t)
	expandTests.RunTestStringFieldAll(t, NewStage)
}

func BenchmarkExpandInDBDepthDepth0MinFull(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkDepth0Min(b, NewStage)
}

func BenchmarkExpandInDBDepthDepth0Full(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkDepth0Full(b, NewStage)
}

func BenchmarkExpandInDBDepthDepth5Full(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkDepth5Full(b, NewStage)
}

func BenchmarkExpandInDBDepthDoubleField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkDoubleField(b, NewStage)
}

func BenchmarkExpandInDBDepthStringField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkStringField(b, NewStage)
}

func BenchmarkExpandInDBDepthBinDataField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkBinDataField(b, NewStage)
}

func BenchmarkExpandInDBDepth_UndefinedField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkUndefinedField(b, NewStage)
}

func BenchmarkExpandInDBDepthBoolField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkBoolField(b, NewStage)
}

func BenchmarkExpandInDBDepthDateField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkDateField(b, NewStage)
}

func BenchmarkExpandInDBDepthNullField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkNullField(b, NewStage)
}

func BenchmarkExpandInDBDepthObjectIdField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkObjectIdField(b, NewStage)
}

func BenchmarkExpandInDBDepthRegexField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkRegexField(b, NewStage)
}

func BenchmarkExpandInDBDepthDbPointerField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkDbPointerField(b, NewStage)
}

func BenchmarkExpandInDBDepthJavascriptField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkJavascriptField(b, NewStage)
}

func BenchmarkExpandInDBDepthSymbolField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkSymbolField(b, NewStage)
}

func BenchmarkExpandInDBDepthJavascriptWithField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkJavascriptWithField(b, NewStage)
}

func BenchmarkExpandInDBDepthIntField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkIntField(b, NewStage)
}

func BenchmarkExpandInDBDepthTimestampField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkTimestampField(b, NewStage)
}

func BenchmarkExpandInDBDepthLongField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkLongField(b, NewStage)
}

func BenchmarkExpandInDBDepthDecimalField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkDecimalField(b, NewStage)
}

func BenchmarkExpandInDBDepthMinKeyField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkMinKeyField(b, NewStage)
}

func BenchmarkExpandInDBDepthMaxKeyField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkMaxKeyField(b, NewStage)
}

func BenchmarkExpandInDBDepthObjectField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkObjectField(b, NewStage)
}

func BenchmarkExpandInDBDepthArrayField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkArrayField(b, NewStage)
}
