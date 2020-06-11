package expandLocally

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand/tests"
	"github.com/mongoeye/mongoeye/decoder"
	"github.com/mongoeye/mongoeye/tests"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestExpandLocallyArrayFieldMinimal(t *testing.T) {
	expandTests.RunTestArrayFieldMinimal(t, NewStage)
}

func TestExpandLocallyArrayFieldLength(t *testing.T) {
	expandTests.RunTestArrayFieldLength(t, NewStage)
}

func TestExpandLocallyArrayFieldValue(t *testing.T) {
	expandTests.RunTestArrayFieldValue(t, NewStage)
}

func TestExpandLocallyArrayFieldArrayMaxLength(t *testing.T) {
	expandTests.RunTestArrayFieldArrayMaxLength(t, NewStage)
}

func TestExpandLocallyArrayFieldMaxDepth(t *testing.T) {
	expandTests.RunTestArrayFieldMaxDepth(t, NewStage)
}

func TestExpandLocallyObjectFieldMinimal(t *testing.T) {
	expandTests.RunTestObjectFieldMinimal(t, NewStage)
}

func TestExpandLocallyObjectFieldLength(t *testing.T) {
	expandTests.RunTestObjectFieldLength(t, NewStage)
}

func TestExpandLocallyObjectFieldMaxDepth(t *testing.T) {
	expandTests.RunTestObjectFieldMaxDepth(t, NewStage)
}

func TestExpandLocallyObjectFieldValue(t *testing.T) {
	expandTests.RunTestObjectFieldValue(t, NewStage)
}

func TestExpandLocallyScalarTypesMinimal(t *testing.T) {
	expandTests.RunTestScalarTypesMinimal(t, NewStage)
}

func TestExpandLocallyScalarTypesValue(t *testing.T) {
	expandTests.RunTestScalarTypesValue(t, NewStage)
}

func TestExpandLocallyStringFieldMinimal(t *testing.T) {
	expandTests.RunTestStringFieldMinimal(t, NewStage)
}

func TestExpandLocallyStringFieldValue(t *testing.T) {
	expandTests.RunTestStringFieldValue(t, NewStage)
}

func TestExpandLocallyStringFieldLength(t *testing.T) {
	expandTests.RunTestStringFieldLength(t, NewStage)
}

func TestExpandLocallyStringFieldMaxLength(t *testing.T) {
	expandTests.RunTestStringFieldMaxLength(t, NewStage)
}

func TestExpandLocallyStringFieldAll(t *testing.T) {
	expandTests.RunTestStringFieldAll(t, NewStage)
}

func TestExpandLocallyInvalidFieldKind(t *testing.T) {
	d := decoder.NewDecoder([]byte("abcdefgh"))

	assert.Panics(t, func() {
		processField("name", 0xEE, d, 0, &expand.Options{}, nil, false)
	})
}

func TestExpandLocallyInvalidChannelType(t *testing.T) {
	stage := NewStage(&expand.Options{})

	ch := make(chan int)

	assert.Panics(t, func() {
		stage.Processor(ch, &analysis.Options{Location: time.UTC, Concurrency: 1, BufferSize: 1, BatchSize: 1})
	})
}

func BenchmarkExpandLocallyDepth0MinFull(b *testing.B) {
	expandTests.RunBenchmarkDepth0Min(b, NewStage)
}

func BenchmarkExpandLocallyDepth0Full(b *testing.B) {
	expandTests.RunBenchmarkDepth0Full(b, NewStage)
}

func BenchmarkExpandLocallyDepth5Full(b *testing.B) {
	expandTests.RunBenchmarkDepth5Full(b, NewStage)
}

func BenchmarkExpandLocallyDoubleField(b *testing.B) {
	expandTests.RunBenchmarkDoubleField(b, NewStage)
}

func BenchmarkExpandLocallyStringField(b *testing.B) {
	expandTests.RunBenchmarkStringField(b, NewStage)
}

func BenchmarkExpandLocallyBinDataField(b *testing.B) {
	expandTests.RunBenchmarkBinDataField(b, NewStage)
}

func BenchmarkExpandLocallyUndefinedField(b *testing.B) {
	expandTests.RunBenchmarkUndefinedField(b, NewStage)
}

func BenchmarkExpandLocallyBoolField(b *testing.B) {
	expandTests.RunBenchmarkBoolField(b, NewStage)
}

func BenchmarkExpandLocallyDateField(b *testing.B) {
	expandTests.RunBenchmarkDateField(b, NewStage)
}

func BenchmarkExpandLocallyNullField(b *testing.B) {
	expandTests.RunBenchmarkNullField(b, NewStage)
}

func BenchmarkExpandLocallyObjectIdField(b *testing.B) {
	expandTests.RunBenchmarkObjectIdField(b, NewStage)
}

func BenchmarkExpandLocallyRegexField(b *testing.B) {
	expandTests.RunBenchmarkRegexField(b, NewStage)
}

func BenchmarkExpandLocallyDbPointerField(b *testing.B) {
	expandTests.RunBenchmarkDbPointerField(b, NewStage)
}

func BenchmarkExpandLocallyJavascriptField(b *testing.B) {
	expandTests.RunBenchmarkJavascriptField(b, NewStage)
}

func BenchmarkExpandLocallySymbolField(b *testing.B) {
	expandTests.RunBenchmarkSymbolField(b, NewStage)
}

func BenchmarkExpandLocallyJavascriptWithField(b *testing.B) {
	expandTests.RunBenchmarkJavascriptWithField(b, NewStage)
}

func BenchmarkExpandLocallyIntField(b *testing.B) {
	expandTests.RunBenchmarkIntField(b, NewStage)
}

func BenchmarkExpandLocallyTimestampField(b *testing.B) {
	expandTests.RunBenchmarkTimestampField(b, NewStage)
}

func BenchmarkExpandLocallyLongField(b *testing.B) {
	expandTests.RunBenchmarkLongField(b, NewStage)
}

func BenchmarkExpandLocallyDecimalField(b *testing.B) {
	tests.SkipBIfNotSupportAggregationAlgorithm(b)
	expandTests.RunBenchmarkDecimalField(b, NewStage)
}

func BenchmarkExpandLocallyMinKeyField(b *testing.B) {
	expandTests.RunBenchmarkMinKeyField(b, NewStage)
}

func BenchmarkExpandLocallyMaxKeyField(b *testing.B) {
	expandTests.RunBenchmarkMaxKeyField(b, NewStage)
}

func BenchmarkExpandLocallyObjectField(b *testing.B) {
	expandTests.RunBenchmarkObjectField(b, NewStage)
}

func BenchmarkExpandLocallyArrayField(b *testing.B) {
	expandTests.RunBenchmarkArrayField(b, NewStage)
}
