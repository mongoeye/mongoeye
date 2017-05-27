package merge

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

func TestToFieldChannel(t *testing.T) {
	rawCh := make(chan []byte, 50)
	grCh := ToFieldChannel(rawCh, time.UTC, 1, 50)

	bsonData := bson.M{
		BsonFieldName: "name",
		BsonCount:     102,
		BsonTypes: []bson.M{
			{
				analysis.BsonFieldType: "null",
				analysis.BsonCount:     102,
			},
		},
	}

	rawData, err := bson.Marshal(bsonData)
	if err != nil {
		panic(err)
	}
	rawCh <- rawData

	close(rawCh)

	expected := analysis.Field{
		Name:  "name",
		Count: 102,
		Types: []*analysis.Type{
			{
				Name:  "null",
				Count: 102,
			},
		},
	}

	result, ok := <-grCh
	assert.Equal(t, expected, result)
	assert.Equal(t, true, ok)
}

func TestToFieldChannel_Full(t *testing.T) {
	rawCh := make(chan []byte, 50)
	grCh := ToFieldChannel(rawCh, time.UTC, 1, 50)

	bsonData := bson.M{
		BsonFieldName: "name",
		BsonCount:     123,
		BsonTypes: []bson.M{
			{
				analysis.BsonFieldType:   "string",
				analysis.BsonCount:       102,
				analysis.BsonCountUnique: 35,
				analysis.BsonValueExtremes: bson.M{
					analysis.BsonMinValue: "012",
					analysis.BsonMaxValue: "xyzax",
					analysis.BsonAvgValue: nil,
				},
				analysis.BsonLengthExtremes: bson.M{
					analysis.BsonMinLength: 3,
					analysis.BsonMaxLength: 5,
					analysis.BsonAvgLength: 3.478,
				},
				analysis.BsonTopNValues: []bson.M{
					{
						analysis.BsonValueFreqValue: "abc",
						analysis.BsonValueFreqCount: 10,
					},
					{
						analysis.BsonValueFreqValue: "012",
						analysis.BsonValueFreqCount: 5,
					},
					{
						analysis.BsonValueFreqValue: "xyz",
						analysis.BsonValueFreqCount: 4,
					},
				},
				analysis.BsonBottomNValues: []bson.M{
					{
						analysis.BsonValueFreqValue: "i",
						analysis.BsonValueFreqCount: 2,
					},
					{
						analysis.BsonValueFreqValue: "j",
						analysis.BsonValueFreqCount: 1,
					},
					{
						analysis.BsonValueFreqValue: "k",
						analysis.BsonValueFreqCount: 1,
					},
				},
				analysis.BsonValueHistogram: bson.M{
					analysis.BsonHistogramStart:      10,
					analysis.BsonHistogramEnd:        20,
					analysis.BsonHistogramRange:      10,
					analysis.BsonHistogramStep:       1,
					analysis.BsonHistogramNumOfSteps: 10,
					analysis.BsonHistogramIntervals: []bson.M{
						{
							analysis.BsonIntervalValue: 0,
							analysis.BsonIntervalCount: 5,
						},
						{
							analysis.BsonIntervalValue: 5,
							analysis.BsonIntervalCount: 7,
						},
						{
							analysis.BsonIntervalValue: 7,
							analysis.BsonIntervalCount: 9,
						},
					},
				},
				analysis.BsonLengthHistogram: bson.M{
					analysis.BsonHistogramStart:      3,
					analysis.BsonHistogramEnd:        6,
					analysis.BsonHistogramRange:      3,
					analysis.BsonHistogramStep:       1,
					analysis.BsonHistogramNumOfSteps: 3,
					analysis.BsonHistogramIntervals: []bson.M{
						{
							analysis.BsonIntervalValue: 0,
							analysis.BsonIntervalCount: 5,
						},
						{
							analysis.BsonIntervalValue: 1,
							analysis.BsonIntervalCount: 2,
						},
						{
							analysis.BsonIntervalValue: 2,
							analysis.BsonIntervalCount: 3,
						},
					},
				},
			},
		},
	}

	rawData, err := bson.Marshal(bsonData)
	if err != nil {
		panic(err)
	}
	rawCh <- rawData

	close(rawCh)

	expected := analysis.Field{
		Name:  "name",
		Count: 123,
		Types: []*analysis.Type{
			{
				Name:        "string",
				Count:       102,
				CountUnique: 35,
				ValueExtremes: &analysis.ValueExtremes{
					Min: "012",
					Max: "xyzax",
					Avg: nil,
				},
				LengthExtremes: &analysis.LengthExtremes{
					Min: 3,
					Max: 5,
					Avg: 3.478,
				},
				TopNValues: []analysis.ValueFreq{
					{
						Value: "abc",
						Count: 10,
					},
					{
						Value: "012",
						Count: 5,
					},
					{
						Value: "xyz",
						Count: 4,
					},
				},
				BottomNValues: []analysis.ValueFreq{
					{
						Value: "i",
						Count: 2,
					},
					{
						Value: "j",
						Count: 1,
					},
					{
						Value: "k",
						Count: 1,
					},
				},
				ValueHistogram: &analysis.Histogram{
					Start:         10,
					End:           20,
					Range:         10,
					Step:          1,
					NumberOfSteps: 10,
					Intervals: analysis.Intervals{
						{
							Interval: 0,
							Count:    5,
						},
						{
							Interval: 5,
							Count:    7,
						},
						{
							Interval: 7,
							Count:    9,
						},
					},
				},
				LengthHistogram: &analysis.Histogram{
					Start:         3,
					End:           6,
					Range:         3,
					Step:          1,
					NumberOfSteps: 3,
					Intervals: analysis.Intervals{
						{
							Interval: 0,
							Count:    5,
						},
						{
							Interval: 1,
							Count:    2,
						},
						{
							Interval: 2,
							Count:    3,
						},
					},
				},
			},
		},
	}

	result, ok := <-grCh
	assert.Equal(t, expected, result)
	assert.Equal(t, true, ok)
}

func TestToFieldChannel_FieldChannel(t *testing.T) {
	ch := make(chan analysis.Field, 50)
	outCh := ToFieldChannel(ch, time.UTC, 1, 50)
	assert.Equal(t, (<-chan analysis.Field)(ch), outCh)
}

func TestToFieldChannel_InvalidType(t *testing.T) {
	ch := make(chan int, 50)
	assert.Panics(t, func() {
		ToFieldChannel(ch, time.UTC, 1, 50)
	})
}

func TestFieldChannelToSlice(t *testing.T) {
	ch := make(chan analysis.Field, 10)
	ch <- analysis.Field{Name: "abc"}
	close(ch)

	slice := FieldChannelToSlice(ch)
	assert.Equal(t, analysis.Fields{{Name: "abc"}}, slice)
}
