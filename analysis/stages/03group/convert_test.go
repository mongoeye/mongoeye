package group

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

func TestToGroupResultChannel_Minimal(t *testing.T) {
	rawCh := make(chan []byte, 50)
	grCh := ToResultChannel(rawCh, time.UTC, 1, 50)

	bsonData := bson.M{
		BsonFieldName:          "name",
		analysis.BsonFieldType: "string",
		analysis.BsonCount:     102,
	}

	rawData, err := bson.Marshal(bsonData)
	if err != nil {
		panic(err)
	}
	rawCh <- rawData

	close(rawCh)

	expected := Result{
		Name: "name",
		Type: analysis.Type{
			Name:  "string",
			Count: 102,
		},
	}

	result, ok := <-grCh
	assert.Equal(t, expected, result)
	assert.Equal(t, true, ok)
}

func TestToGroupResultChannel_Full(t *testing.T) {
	rawCh := make(chan []byte, 50)
	grCh := ToResultChannel(rawCh, time.UTC, 1, 50)

	bsonData := bson.M{
		BsonFieldName:            "name",
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
	}

	rawData, err := bson.Marshal(bsonData)
	if err != nil {
		panic(err)
	}
	rawCh <- rawData

	close(rawCh)

	expected := Result{
		Name: "name",
		Type: analysis.Type{
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
	}

	result, ok := <-grCh
	assert.Equal(t, expected, result)
	assert.Equal(t, true, ok)
}

func TestToGroupResultChannel_GroupResult(t *testing.T) {
	inCh := make(chan Result, 50)
	outCh := ToResultChannel(inCh, time.UTC, 1, 50)
	assert.Equal(t, (<-chan Result)(inCh), outCh)
}

func TestToGroupResultChannel_InvalidType(t *testing.T) {
	ch := make(chan int, 50)
	assert.Panics(t, func() {
		ToResultChannel(ch, time.UTC, 1, 50)
	})
}

func TestGroupResultChannelToSlice(t *testing.T) {
	ch := make(chan Result, 10)
	ch <- Result{Name: "abc"}
	close(ch)

	slice := ResultChannelToSlice(ch)
	assert.Equal(t, []interface{}{Result{Name: "abc"}}, slice)
}

func TestNormalizeType_Int(t *testing.T) {
	data := &analysis.Type{
		Name: "int",
		ValueHistogram: &analysis.Histogram{
			Start: 12.0,
			End:   21.0,
		},
	}

	NormalizeType(data, time.UTC)

	assert.Equal(t, 12, data.ValueHistogram.Start)
	assert.Equal(t, 21, data.ValueHistogram.End)
}

func TestNormalizeType_Long(t *testing.T) {
	data := &analysis.Type{
		Name: "long",
		ValueHistogram: &analysis.Histogram{
			Start: 12.0,
			End:   21.0,
		},
	}

	NormalizeType(data, time.UTC)

	assert.Equal(t, int64(12), data.ValueHistogram.Start)
	assert.Equal(t, int64(21), data.ValueHistogram.End)
}

func TestNormalizeType_Date(t *testing.T) {
	data := &analysis.Type{
		Name: "date",
		ValueExtremes: &analysis.ValueExtremes{
			Min: helpers.ParseDate("2017-04-17T23:59:59+00:00"),
			Max: helpers.ParseDate("2017-04-21T00:00:01+00:00"),
		},
		ValueHistogram: &analysis.Histogram{
			Start: helpers.ParseDate("2017-04-15T23:59:59+00:00"),
			End:   helpers.ParseDate("2017-04-22T00:00:01+00:00"),
		},
		TopNValues: analysis.ValueFreqSlice{
			{
				Value: helpers.ParseDate("2017-04-25T00:00:01+00:00"),
				Count: 10,
			},
		},
		BottomNValues: analysis.ValueFreqSlice{
			{
				Value: helpers.ParseDate("2017-04-10T00:00:01+00:00"),
				Count: 1,
			},
		},
	}

	loc, _ := time.LoadLocation("America/New_York")
	NormalizeType(data, loc)

	assert.NotEqual(t, data.ValueExtremes.Min, helpers.ParseDate("2017-04-17T23:59:59+00:00"))
	assert.Equal(t, data.ValueExtremes.Min, helpers.ParseDate("2017-04-17T23:59:59+00:00").In(loc))
	assert.NotEqual(t, data.ValueExtremes.Max, helpers.ParseDate("2017-04-21T00:00:01+00:00"))
	assert.Equal(t, data.ValueExtremes.Max, helpers.ParseDate("2017-04-21T00:00:01+00:00").In(loc))
	assert.NotEqual(t, data.ValueHistogram.Start, helpers.ParseDate("2017-04-15T23:59:59+00:00"))
	assert.Equal(t, data.ValueHistogram.Start, helpers.ParseDate("2017-04-15T23:59:59+00:00").In(loc))
	assert.NotEqual(t, data.ValueHistogram.End, helpers.ParseDate("2017-04-22T00:00:01+00:00"))
	assert.Equal(t, data.ValueHistogram.End, helpers.ParseDate("2017-04-22T00:00:01+00:00").In(loc))

	assert.NotEqual(t, data.TopNValues[0].Value, helpers.ParseDate("2017-04-25T00:00:01+00:00"))
	assert.Equal(t, data.TopNValues[0].Value, helpers.ParseDate("2017-04-25T00:00:01+00:00").In(loc))
	assert.NotEqual(t, data.BottomNValues[0].Value, helpers.ParseDate("2017-04-10T00:00:01+00:00"))
	assert.Equal(t, data.BottomNValues[0].Value, helpers.ParseDate("2017-04-10T00:00:01+00:00").In(loc))
}

func TestNormalizeType_Decimal(t *testing.T) {
	data := &analysis.Type{
		Name: "decimal",
		ValueHistogram: &analysis.Histogram{
			Start: 12.0,
			End:   21.0,
		},
	}

	NormalizeType(data, time.UTC)

	assert.Equal(t, helpers.ParseDecimal("12"), data.ValueHistogram.Start)
	assert.Equal(t, helpers.ParseDecimal("21"), data.ValueHistogram.End)
}
