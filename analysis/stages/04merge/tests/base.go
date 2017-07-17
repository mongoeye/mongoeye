package mergeTests

import (
	"github.com/jinzhu/copier"
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/04merge"
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/mongoeye/mongoeye/tests"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

// RunTestAllTypes tests merge stage on all MongoDB types.
func RunTestAllTypes(t *testing.T, stageFactory merge.StageFactory) {
	c := setup()
	defer tearDown(c)

	// Decimal type is new in version 3.4
	decimalType := "decimal"
	var decimal1 interface{} = helpers.ParseDecimal("123.3")
	var decimal2 interface{} = helpers.ParseDecimal("456.9")
	var decimal3 interface{} = helpers.ParseDecimal("789.3")
	var decimalAvg interface{} = helpers.ParseDecimal("456.5")
	var decimalHistogram = &analysis.Histogram{
		Start:         helpers.ParseDecimal("123.0"),
		End:           helpers.ParseDecimal("791.0"),
		Range:         668,
		Step:          1,
		NumberOfSteps: 668,
		Intervals: analysis.Intervals{
			{
				Interval: 0,
				Count:    1,
			},
			{
				Interval: 333,
				Count:    1,
			},
			{
				Interval: 666,
				Count:    1,
			},
		},
	}
	var decimalLengthStats *analysis.LengthStats

	if !tests.HasMongoDBDecimalSupport() {
		decimalType = "string"
		decimal1 = "123.3"
		decimal2 = "456.9"
		decimal3 = "789.3"
		decimalAvg = nil
		decimalHistogram = nil
		decimalLengthStats = &analysis.LengthStats{
			Min: 5,
			Max: 5,
			Avg: 5.0,
		}
	}

	c.Insert(bson.M{
		"_id":                  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_double":              123.456,
		"_string":              "Šašo",
		"_binData":             []byte("Šašo"),
		"_undefined":           bson.Undefined,
		"_bool":                false,
		"_date":                time.Unix(1490959230, 0),
		"_null":                nil,
		"_regex":               bson.RegEx{Pattern: "a.*", Options: "i"},
		"_dbPointer":           bson.DBPointer{Namespace: "ns", Id: bson.ObjectIdHex("58de3f123d9654ba801bb30d")},
		"_javascript":          bson.JavaScript{Code: "var x = 1+1;", Scope: nil},
		"_symbol":              bson.Symbol("x"),
		"_javascriptWithScope": bson.JavaScript{Code: "var x = 1+y;", Scope: bson.M{"y": 5}},
		"_int":                 123,
		"_timestamp":           bson.MongoTimestamp(1490959230),
		"_long":                int64(456),
		"_decimal":             decimal1,
		"_minKey":              bson.MinKey,
		"_maxKey":              bson.MaxKey,
		"_object": bson.M{
			"f1": 10,
			"f2": "abc",
		},
	})
	c.Insert(bson.M{
		"_id":                  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"_double":              456.789,
		"_string":              "abc",
		"_binData":             []byte("abc"),
		"_undefined":           bson.Undefined,
		"_bool":                true,
		"_date":                time.Unix(1490959231, 0),
		"_null":                nil,
		"_regex":               bson.RegEx{Pattern: "b.*", Options: "i"},
		"_dbPointer":           bson.DBPointer{Namespace: "ns", Id: bson.ObjectIdHex("58de3f123d9654ba801bb31d")},
		"_javascript":          bson.JavaScript{Code: "var x = 2+2;", Scope: nil},
		"_symbol":              bson.Symbol("x"),
		"_javascriptWithScope": bson.JavaScript{Code: "var x = 2+y;", Scope: bson.M{"y": 5}},
		"_int":                 123,
		"_timestamp":           bson.MongoTimestamp(1490959230),
		"_long":                int64(456),
		"_decimal":             decimal2,
		"_minKey":              bson.MinKey,
		"_maxKey":              bson.MaxKey,
		"_object": bson.M{
			"f1": "cde",
			"f2": 20,
		},
		"_array": []interface{}{1, 2},
	})
	c.Insert(bson.M{
		"_id":                  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_double":              798.123,
		"_string":              "cde",
		"_binData":             []byte("cde"),
		"_undefined":           bson.Undefined,
		"_bool":                true,
		"_date":                time.Unix(1490959232, 0),
		"_null":                nil,
		"_regex":               bson.RegEx{Pattern: "c.*", Options: "i"},
		"_dbPointer":           bson.DBPointer{Namespace: "ns", Id: bson.ObjectIdHex("58de3f123d9654ba801bb32d")},
		"_javascript":          bson.JavaScript{Code: "var x = 3+3;", Scope: nil},
		"_symbol":              bson.Symbol("y"),
		"_javascriptWithScope": bson.JavaScript{Code: "var x = 3+y;", Scope: bson.M{"y": 5}},
		"_int":                 456,
		"_timestamp":           bson.MongoTimestamp(1490959231),
		"_long":                int64(123),
		"_decimal":             decimal3,
		"_minKey":              bson.MinKey,
		"_maxKey":              bson.MaxKey,
		"_array":               []interface{}{3, 4},
	})

	options := merge.Options{}
	copier.Copy(&options, testMergeOptions)

	expected := []interface{}{
		&analysis.Field{
			Name:  "_id",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:  "objectId",
					Count: 3,
					ValueStats: &analysis.ValueStats{
						Max: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
						Min: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
						Avg: nil,
					},
				},
			},
		},
		&analysis.Field{
			Name:  "_double",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:        "double",
					Count:       3,
					CountUnique: 3,
					ValueStats: &analysis.ValueStats{
						Min: 123.456,
						Max: 798.123,
						Avg: 459.45599999999996,
					},
					MostFrequent: []analysis.ValueFreq{
						{
							Count: 1,
							Value: 123.456,
						},
						{
							Count: 1,
							Value: 456.789,
						},
						{
							Count: 1,
							Value: 798.123,
						},
					},
					LeastFrequent: []analysis.ValueFreq{
						{
							Count: 1,
							Value: 123.456,
						},
						{
							Count: 1,
							Value: 456.789,
						},
						{
							Count: 1,
							Value: 798.123,
						},
					},
					ValueHistogram: &analysis.Histogram{
						Start:         123,
						End:           800,
						Range:         677,
						Step:          1,
						NumberOfSteps: 677,
						Intervals: analysis.Intervals{
							{
								Interval: 0,
								Count:    1,
							},
							{
								Interval: 333,
								Count:    1,
							},
							{
								Interval: 675,
								Count:    1,
							},
						},
					},
				},
			},
		},
		&analysis.Field{
			Name:  "_string",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:        "string",
					Count:       3,
					CountUnique: 3,
					ValueStats: &analysis.ValueStats{
						Max: "Šašo",
						Min: "abc",
						Avg: nil,
					},
					LengthStats: &analysis.LengthStats{
						Max: 4,
						Min: 3,
						Avg: 3.3333333333333335,
					},
					MostFrequent: []analysis.ValueFreq{
						{
							Count: 1,
							Value: "Šašo",
						},
						{
							Count: 1,
							Value: "abc",
						},
						{
							Count: 1,
							Value: "cde",
						},
					},
					LeastFrequent: []analysis.ValueFreq{
						{
							Count: 1,
							Value: "Šašo",
						},
						{
							Count: 1,
							Value: "abc",
						},
						{
							Count: 1,
							Value: "cde",
						},
					},
					LengthHistogram: &analysis.Histogram{
						Start:         3,
						End:           5,
						Range:         2,
						Step:          1,
						NumberOfSteps: 2,
						Intervals: analysis.Intervals{
							{
								Interval: 0,
								Count:    2,
							},
							{
								Interval: 1,
								Count:    1,
							},
						},
					},
				},
			},
		},
		&analysis.Field{
			Name:  "_binData",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:  "binData",
					Count: 3,
				},
			},
		},
		&analysis.Field{
			Name:  "_undefined",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:  "undefined",
					Count: 3,
				},
			},
		},
		&analysis.Field{
			Name:  "_bool",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:  "bool",
					Count: 3,
					ValueStats: &analysis.ValueStats{
						Min: false,
						Max: true,
						Avg: 0.6666666666666666,
					},
				},
			},
		},
		&analysis.Field{
			Name:  "_date",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:        "date",
					Count:       3,
					CountUnique: 3,
					ValueStats: &analysis.ValueStats{
						Min: helpers.ParseDate("2017-03-31T11:20:32+00:00"),
						Max: helpers.ParseDate("2017-03-31T11:20:30+00:00"),
						Avg: nil,
					},
					MostFrequent: []analysis.ValueFreq{
						{
							Value: helpers.ParseDate("2017-03-31T11:20:30+00:00"),
							Count: 1,
						},
						{
							Value: helpers.ParseDate("2017-03-31T11:20:31+00:00"),
							Count: 1,
						},
						{
							Value: helpers.ParseDate("2017-03-31T11:20:32+00:00"),
							Count: 1,
						},
					},
					LeastFrequent: []analysis.ValueFreq{
						{
							Value: helpers.ParseDate("2017-03-31T11:20:30+00:00"),
							Count: 1,
						},
						{
							Value: helpers.ParseDate("2017-03-31T11:20:31+00:00"),
							Count: 1,
						},
						{
							Value: helpers.ParseDate("2017-03-31T11:20:32+00:00"),
							Count: 1,
						},
					},
					ValueHistogram: &analysis.Histogram{
						Start:         helpers.ParseDate("2017-03-31T11:20:30+00:00"),
						End:           helpers.ParseDate("2017-03-31T11:20:33+00:00"),
						Range:         3,
						Step:          1,
						NumberOfSteps: 3,
						Intervals: analysis.Intervals{
							{
								Interval: 0,
								Count:    1,
							},
							{
								Interval: 1,
								Count:    1,
							},
							{
								Interval: 2,
								Count:    1,
							},
						},
					},
					WeekdayHistogram: &analysis.WeekdayHistogram{
						0,
						0,
						0,
						0,
						0,
						3,
						0,
					},
					HourHistogram: &analysis.HourHistogram{
						0,
						0,
						0,
						0,
						0,
						0,
						0,
						0,
						0,
						0,
						0,
						3,
						0,
						0,
						0,
						0,
						0,
						0,
						0,
						0,
						0,
						0,
						0,
						0,
					},
				},
			},
		},
		&analysis.Field{
			Name:  "_null",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:  "null",
					Count: 3,
				},
			},
		},
		&analysis.Field{
			Name:  "_regex",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:  "regex",
					Count: 3,
				},
			},
		},
		&analysis.Field{
			Name:  "_dbPointer",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:  "dbPointer",
					Count: 3,
				},
			},
		},
		&analysis.Field{
			Name:  "_javascript",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:  "javascript",
					Count: 3,
				},
			},
		},
		&analysis.Field{
			Name:  "_symbol",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:  "symbol",
					Count: 3,
				},
			},
		},
		&analysis.Field{
			Name:  "_javascriptWithScope",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:  "javascriptWithScope",
					Count: 3,
				},
			},
		},
		&analysis.Field{
			Name:  "_int",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:        "int",
					Count:       3,
					CountUnique: 2,
					ValueStats: &analysis.ValueStats{
						Min: 123,
						Max: 456,
						Avg: 234.0,
					},
					MostFrequent: []analysis.ValueFreq{
						{
							Value: 123,
							Count: 2,
						},
						{
							Value: 456,
							Count: 1,
						},
					},
					LeastFrequent: []analysis.ValueFreq{
						{
							Value: 123,
							Count: 2,
						},
						{
							Value: 456,
							Count: 1,
						},
					},
					ValueHistogram: &analysis.Histogram{
						Start:         123,
						End:           457,
						Range:         334,
						Step:          1,
						NumberOfSteps: 334,
						Intervals: analysis.Intervals{
							{
								Interval: 0,
								Count:    2,
							},
							{
								Interval: 333,
								Count:    1,
							},
						},
					},
				},
			},
		},
		&analysis.Field{
			Name:  "_timestamp",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:        "timestamp",
					Count:       3,
					CountUnique: 2,
					ValueStats: &analysis.ValueStats{
						Min: 1490959230,
						Max: 1490959231,
						Avg: nil,
					},
					MostFrequent: []analysis.ValueFreq{
						{
							Value: 1490959230,
							Count: 2,
						},
						{
							Value: 1490959231,
							Count: 1,
						},
					},
					LeastFrequent: []analysis.ValueFreq{
						{
							Value: 1490959230,
							Count: 2,
						},
						{
							Value: 1490959231,
							Count: 1,
						},
					},
				},
			},
		},
		&analysis.Field{
			Name:  "_long",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:        "long",
					Count:       3,
					CountUnique: 2,
					ValueStats: &analysis.ValueStats{
						Min: 123,
						Max: 456,
						Avg: 345.0,
					},
					MostFrequent: []analysis.ValueFreq{
						{
							Value: 456,
							Count: 2,
						},
						{
							Value: 123,
							Count: 1,
						},
					},
					LeastFrequent: []analysis.ValueFreq{
						{
							Value: 456,
							Count: 2,
						},
						{
							Value: 123,
							Count: 1,
						},
					},
					ValueHistogram: &analysis.Histogram{
						Start:         123,
						End:           457,
						Range:         334,
						Step:          1,
						NumberOfSteps: 334,
						Intervals: analysis.Intervals{
							{
								Interval: 0,
								Count:    1,
							},
							{
								Interval: 333,
								Count:    2,
							},
						},
					},
				},
			},
		},
		&analysis.Field{
			Name:  "_decimal",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:        decimalType,
					Count:       3,
					CountUnique: 3,
					ValueStats: &analysis.ValueStats{
						Min: decimal1,
						Max: decimal3,
						Avg: decimalAvg,
					},
					LengthStats: decimalLengthStats,
					MostFrequent: []analysis.ValueFreq{
						{
							Value: decimal1,
							Count: 1,
						},
						{
							Value: decimal2,
							Count: 1,
						},
						{
							Value: decimal3,
							Count: 1,
						},
					},
					LeastFrequent: []analysis.ValueFreq{
						{
							Value: decimal1,
							Count: 1,
						},
						{
							Value: decimal2,
							Count: 1,
						},
						{
							Value: decimal3,
							Count: 1,
						},
					},
					ValueHistogram: decimalHistogram,
				},
			},
		},
		&analysis.Field{
			Name:  "_minKey",
			Count: 3,
			Types: analysis.Types{
				{
					Name:  "minKey",
					Count: 3,
				},
			},
		},
		&analysis.Field{
			Name:  "_maxKey",
			Level: 0,
			Count: 3,
			Types: analysis.Types{
				{
					Name:  "maxKey",
					Count: 3,
				},
			},
		},
		&analysis.Field{
			Name:  "_array",
			Level: 0,
			Count: 2,
			Types: analysis.Types{
				{
					Name:  "array",
					Count: 2,
					LengthStats: &analysis.LengthStats{
						Min: 2,
						Max: 2,
						Avg: 2.0,
					},
				},
			},
		},
		&analysis.Field{
			Name:  "_array.[]",
			Level: 1,
			Count: 4,
			Types: analysis.Types{
				{
					Name:        "int",
					Count:       4,
					CountUnique: 4,
					ValueStats: &analysis.ValueStats{
						Min: 1,
						Max: 4,
						Avg: 2.5,
					},
					MostFrequent: []analysis.ValueFreq{
						{
							Value: 1,
							Count: 1,
						},
						{
							Value: 2,
							Count: 1,
						},
						{
							Value: 3,
							Count: 1,
						},
						{
							Value: 4,
							Count: 1,
						},
					},
					LeastFrequent: []analysis.ValueFreq{
						{
							Value: 1,
							Count: 1,
						},
						{
							Value: 2,
							Count: 1,
						},
						{
							Value: 3,
							Count: 1,
						},
						{
							Value: 4,
							Count: 1,
						},
					},
					ValueHistogram: &analysis.Histogram{
						Start:         1,
						End:           5,
						Range:         4,
						Step:          1,
						NumberOfSteps: 4,
						Intervals: analysis.Intervals{
							{
								Interval: 0,
								Count:    1,
							},
							{
								Interval: 1,
								Count:    1,
							},
							{
								Interval: 2,
								Count:    1,
							},
							{
								Interval: 3,
								Count:    1,
							},
						},
					},
				},
			},
		},
		&analysis.Field{
			Name:  "_object",
			Level: 0,
			Count: 2,
			Types: analysis.Types{
				{
					Name:  "object",
					Count: 2,
					LengthStats: &analysis.LengthStats{
						Min: 2,
						Max: 2,
						Avg: 2.0,
					},
				},
			},
		},
		&analysis.Field{
			Name:  "_object.f1",
			Level: 1,
			Count: 2,
			Types: analysis.Types{
				{
					Name:        "string",
					Count:       1,
					CountUnique: 1,
					ValueStats: &analysis.ValueStats{
						Min: "cde",
						Max: "cde",
						Avg: nil,
					},
					LengthStats: &analysis.LengthStats{
						Min: 3,
						Max: 3,
						Avg: 3.0,
					},
					MostFrequent: []analysis.ValueFreq{
						{
							Count: 1,
							Value: "cde",
						},
					},
					LeastFrequent: []analysis.ValueFreq{
						{
							Count: 1,
							Value: "cde",
						},
					},
				},
				{
					Name:        "int",
					Count:       1,
					CountUnique: 1,
					ValueStats: &analysis.ValueStats{
						Min: 10,
						Max: 10,
						Avg: 10.0,
					},
					MostFrequent: []analysis.ValueFreq{
						{
							Count: 1,
							Value: 10,
						},
					},
					LeastFrequent: []analysis.ValueFreq{
						{
							Count: 1,
							Value: 10,
						},
					},
				},
			},
		},
		&analysis.Field{
			Name:  "_object.f2",
			Level: 1,
			Count: 2,
			Types: analysis.Types{
				{
					Name:        "string",
					Count:       1,
					CountUnique: 1,
					ValueStats: &analysis.ValueStats{
						Min: "abc",
						Max: "abc",
						Avg: nil,
					},
					LengthStats: &analysis.LengthStats{
						Min: 3,
						Max: 3,
						Avg: 3.0,
					},
					MostFrequent: []analysis.ValueFreq{
						{
							Count: 1,
							Value: "abc",
						},
					},
					LeastFrequent: []analysis.ValueFreq{
						{
							Count: 1,
							Value: "abc",
						},
					},
				},
				{
					Name:        "int",
					Count:       1,
					CountUnique: 1,
					ValueStats: &analysis.ValueStats{
						Min: 20,
						Max: 20,
						Avg: 20.0,
					},
					MostFrequent: []analysis.ValueFreq{
						{
							Count: 1,
							Value: 20,
						},
					},
					LeastFrequent: []analysis.ValueFreq{
						{
							Count: 1,
							Value: 20,
						},
					},
				},
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}
