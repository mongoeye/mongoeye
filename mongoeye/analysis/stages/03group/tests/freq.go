package groupTests

import (
	"github.com/jinzhu/copier"
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/mongoeye/mongoeye/tests"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

// RunTestValueHistogram tests results of ValueHistogramMaxRes option.
func RunTestValueHistogram(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	// Decimal type is new in version 3.4
	decimalType := "decimal"
	var decimal1 interface{} = helpers.ParseDecimal("123")
	var decimal2 interface{} = helpers.ParseDecimal("456")
	var decimal3 interface{} = helpers.ParseDecimal("789")
	decimalHistogram := &analysis.Histogram{
		Start:         helpers.ParseDecimal("123"),
		End:           helpers.ParseDecimal("790"),
		Range:         667,
		Step:          1,
		NumberOfSteps: 667,
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
	if !tests.HasMongoDBDecimalSupport() {
		decimalType = "string"
		decimal1 = "123"
		decimal2 = "456"
		decimal3 = "789"
		decimalHistogram = nil
	}

	c.Insert(bson.M{
		"_id":                  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_double":              123.456,
		"_string":              "Šašo",
		"_binData":             []byte("Šašo"),
		"_undefined":           bson.Undefined,
		"_bool":                false,
		"_date":                helpers.ParseDate("2017-01-01T00:00:00+00:00"),
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
		"_date":                helpers.ParseDate("2017-01-03T00:00:00+00:00"),
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
		"_double":              123.456,
		"_string":              "Šašo",
		"_binData":             []byte("abc"),
		"_undefined":           bson.Undefined,
		"_bool":                true,
		"_date":                helpers.ParseDate("2017-01-02T00:00:00+00:00"),
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

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.ValueHistogramMaxRes = 1000

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 3,
			},
		},
		group.Result{
			Name: "_double",
			Type: analysis.Type{
				Name:  "double",
				Count: 3,
				ValueHistogram: &analysis.Histogram{
					Start:         123,
					End:           457.5,
					Range:         334.5,
					Step:          0.5,
					NumberOfSteps: 669,
					Intervals: analysis.Intervals{
						{
							Interval: 0,
							Count:    2,
						},
						{
							Interval: 667,
							Count:    1,
						},
					},
				},
			},
		},
		group.Result{
			Name: "_string",
			Type: analysis.Type{
				Name:  "string",
				Count: 3,
			},
		},
		group.Result{
			Name: "_binData",
			Type: analysis.Type{
				Name:  "binData",
				Count: 3,
			},
		},
		group.Result{
			Name: "_undefined",
			Type: analysis.Type{
				Name:  "undefined",
				Count: 3,
			},
		},
		group.Result{
			Name: "_bool",
			Type: analysis.Type{
				Name:  "bool",
				Count: 3,
			},
		},
		group.Result{
			Name: "_date",
			Type: analysis.Type{
				Name:  "date",
				Count: 3,
				ValueHistogram: &analysis.Histogram{
					Start:         helpers.ParseDate("2017-01-01T00:00:00+00:00"),
					End:           helpers.ParseDate("2017-01-03T00:05:00+00:00"),
					Range:         173100,
					Step:          300,
					NumberOfSteps: 577,
					Intervals: analysis.Intervals{
						{
							Interval: 0,
							Count:    1,
						},
						{
							Interval: 288,
							Count:    1,
						},
						{
							Interval: 576,
							Count:    1,
						},
					},
				},
			},
		},
		group.Result{
			Name: "_null",
			Type: analysis.Type{
				Name:  "null",
				Count: 3,
			},
		},
		group.Result{
			Name: "_regex",
			Type: analysis.Type{
				Name:  "regex",
				Count: 3,
			},
		},
		group.Result{
			Name: "_dbPointer",
			Type: analysis.Type{
				Name:  "dbPointer",
				Count: 3,
			},
		},
		group.Result{
			Name: "_javascript",
			Type: analysis.Type{
				Name:  "javascript",
				Count: 3,
			},
		},
		group.Result{
			Name: "_symbol",
			Type: analysis.Type{
				Name:  "symbol",
				Count: 3,
			},
		},
		group.Result{
			Name: "_javascriptWithScope",
			Type: analysis.Type{
				Name:  "javascriptWithScope",
				Count: 3,
			},
		},
		group.Result{
			Name: "_int",
			Type: analysis.Type{
				Name:  "int",
				Count: 3,
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
		group.Result{
			Name: "_timestamp",
			Type: analysis.Type{
				Name:  "timestamp",
				Count: 3,
			},
		},
		group.Result{
			Name: "_long",
			Type: analysis.Type{
				Name:  "long",
				Count: 3,
				ValueHistogram: &analysis.Histogram{
					Start:         int64(123),
					End:           int64(457),
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
		group.Result{
			Name: "_decimal",
			Type: analysis.Type{
				Name:           decimalType,
				Count:          3,
				ValueHistogram: decimalHistogram,
			},
		},
		group.Result{
			Name: "_minKey",
			Type: analysis.Type{
				Name:  "minKey",
				Count: 3,
			},
		},
		group.Result{
			Name: "_maxKey",
			Type: analysis.Type{
				Name:  "maxKey",
				Count: 3,
			},
		},
		group.Result{
			Name: "_object",
			Type: analysis.Type{
				Name:  "object",
				Count: 2,
			},
		},
		group.Result{
			Name: "_object.f1",
			Type: analysis.Type{
				Name:  "int",
				Count: 1,
			},
		},
		group.Result{
			Name: "_object.f1",
			Type: analysis.Type{
				Name:  "string",
				Count: 1,
			},
		},
		group.Result{
			Name: "_object.f2",
			Type: analysis.Type{
				Name:  "string",
				Count: 1,
			},
		},
		group.Result{
			Name: "_object.f2",
			Type: analysis.Type{
				Name:  "int",
				Count: 1,
			},
		},
		group.Result{
			Name: "_array",
			Type: analysis.Type{
				Name:  "array",
				Count: 2,
			},
		},
		group.Result{
			Name: "_array.[]",
			Type: analysis.Type{
				Name:  "int",
				Count: 4,
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
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}

// RunTestValueHistogramMaxRes tests group stage with ValueHistogramMaxRes option.
func RunTestValueHistogramMaxRes(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"_int": 0,
	})
	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_int": 1,
	})
	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_int": 2,
	})
	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
		"_int": 3,
	})
	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c4"),
		"_int": 4,
	})
	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c5"),
		"_int": 5,
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.ValueHistogramMaxRes = 5

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 6,
			},
		},
		group.Result{
			Name: "_int",
			Type: analysis.Type{
				Name:  "int",
				Count: 6,
				ValueHistogram: &analysis.Histogram{
					Start:         0,
					End:           9,
					Step:          3.0,
					Range:         9.0,
					NumberOfSteps: 3,
					Intervals: analysis.Intervals{
						{
							Interval: 0,
							Count:    3,
						},
						{
							Interval: 1,
							Count:    3,
						},
					},
				},
			},
		},
	}

	stage := stageFactory(&options)

	testStage(t, c, time.UTC, stage, expected)
}

// RunTestLengthHistogram tests results of LengthHistogramMaxRes option.
func RunTestLengthHistogram(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	// Decimal type is new in version 3.4
	decimalType := "decimal"
	var decimal1 interface{} = helpers.ParseDecimal("123")
	var decimal2 interface{} = helpers.ParseDecimal("456")
	var decimal3 interface{} = helpers.ParseDecimal("789")
	if !tests.HasMongoDBDecimalSupport() {
		decimalType = "string"
		decimal1 = "123"
		decimal2 = "456"
		decimal3 = "789"
	}

	c.Insert(bson.M{
		"_id":                  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"_double":              123.456,
		"_string":              "Šašo",
		"_binData":             []byte("Šašo"),
		"_undefined":           bson.Undefined,
		"_bool":                true,
		"_date":                helpers.ParseDate("2017-01-01T00:00:00+00:00"),
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
		"_id":                  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_double":              456.789,
		"_string":              "abc",
		"_binData":             []byte("abc"),
		"_undefined":           bson.Undefined,
		"_bool":                false,
		"_date":                helpers.ParseDate("2017-01-03T00:00:00+00:00"),
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
		"_array": []interface{}{1},
	})
	c.Insert(bson.M{
		"_id":                  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_double":              123.456,
		"_string":              "Šašo",
		"_binData":             []byte("abc"),
		"_undefined":           bson.Undefined,
		"_bool":                true,
		"_date":                helpers.ParseDate("2017-01-02T00:00:00+00:00"),
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
		"_array":               []interface{}{3, 4, 5},
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.LengthHistogramMaxRes = 100

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 3,
			},
		},
		group.Result{
			Name: "_double",
			Type: analysis.Type{
				Name:  "double",
				Count: 3,
			},
		},
		group.Result{
			Name: "_string",
			Type: analysis.Type{
				Name:  "string",
				Count: 3,
				LengthHistogram: &analysis.Histogram{
					Start:         3,
					End:           5,
					Range:         2,
					Step:          1,
					NumberOfSteps: 2,
					Intervals: analysis.Intervals{
						{
							Interval: 0,
							Count:    1,
						},
						{
							Interval: 1,
							Count:    2,
						},
					},
				},
			},
		},
		group.Result{
			Name: "_binData",
			Type: analysis.Type{
				Name:  "binData",
				Count: 3,
			},
		},
		group.Result{
			Name: "_undefined",
			Type: analysis.Type{
				Name:  "undefined",
				Count: 3,
			},
		},
		group.Result{
			Name: "_bool",
			Type: analysis.Type{
				Name:  "bool",
				Count: 3,
			},
		},
		group.Result{
			Name: "_date",
			Type: analysis.Type{
				Name:  "date",
				Count: 3,
			},
		},
		group.Result{
			Name: "_null",
			Type: analysis.Type{
				Name:  "null",
				Count: 3,
			},
		},
		group.Result{
			Name: "_regex",
			Type: analysis.Type{
				Name:  "regex",
				Count: 3,
			},
		},
		group.Result{
			Name: "_dbPointer",
			Type: analysis.Type{
				Name:  "dbPointer",
				Count: 3,
			},
		},
		group.Result{
			Name: "_javascript",
			Type: analysis.Type{
				Name:  "javascript",
				Count: 3,
			},
		},
		group.Result{
			Name: "_symbol",
			Type: analysis.Type{
				Name:  "symbol",
				Count: 3,
			},
		},
		group.Result{
			Name: "_javascriptWithScope",
			Type: analysis.Type{
				Name:  "javascriptWithScope",
				Count: 3,
			},
		},
		group.Result{
			Name: "_int",
			Type: analysis.Type{
				Name:  "int",
				Count: 3,
			},
		},
		group.Result{
			Name: "_timestamp",
			Type: analysis.Type{
				Name:  "timestamp",
				Count: 3,
			},
		},
		group.Result{
			Name: "_long",
			Type: analysis.Type{
				Name:  "long",
				Count: 3,
			},
		},
		group.Result{
			Name: "_decimal",
			Type: analysis.Type{
				Name:  decimalType,
				Count: 3,
			},
		},
		group.Result{
			Name: "_minKey",
			Type: analysis.Type{
				Name:  "minKey",
				Count: 3,
			},
		},
		group.Result{
			Name: "_maxKey",
			Type: analysis.Type{
				Name:  "maxKey",
				Count: 3,
			},
		},
		group.Result{
			Name: "_object",
			Type: analysis.Type{
				Name:  "object",
				Count: 2,
			},
		},
		group.Result{
			Name: "_object.f1",
			Type: analysis.Type{
				Name:  "int",
				Count: 1,
			},
		},
		group.Result{
			Name: "_object.f1",
			Type: analysis.Type{
				Name:  "string",
				Count: 1,
			},
		},
		group.Result{
			Name: "_object.f2",
			Type: analysis.Type{
				Name:  "string",
				Count: 1,
			},
		},
		group.Result{
			Name: "_object.f2",
			Type: analysis.Type{
				Name:  "int",
				Count: 1,
			},
		},
		group.Result{
			Name: "_array",
			Type: analysis.Type{
				Name:  "array",
				Count: 2,
				LengthHistogram: &analysis.Histogram{
					Start:         1,
					End:           4,
					Range:         3,
					Step:          1,
					NumberOfSteps: 3,
					Intervals: analysis.Intervals{
						{
							Interval: 0,
							Count:    1,
						},
						{
							Interval: 2,
							Count:    1,
						},
					},
				},
			},
		},
		group.Result{
			Name: "_array.[]",
			Type: analysis.Type{
				Name:  "int",
				Count: 4,
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}

// RunTestLengthHistogramMaxRes tests group stage with ValueHistogramMaxRes option.
func RunTestLengthHistogramMaxRes(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"_str": "",
	})
	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_str": "1",
	})
	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_str": "12",
	})
	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
		"_str": "123",
	})
	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c4"),
		"_str": "1234",
	})
	c.Insert(bson.M{
		"_id":  bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c5"),
		"_str": "12345",
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.LengthHistogramMaxRes = 5

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 6,
			},
		},
		group.Result{
			Name: "_str",
			Type: analysis.Type{
				Name:  "string",
				Count: 6,
				LengthHistogram: &analysis.Histogram{
					Start:         0,
					End:           9,
					Step:          3.0,
					Range:         9.0,
					NumberOfSteps: 3,
					Intervals: analysis.Intervals{
						{
							Interval: 0,
							Count:    3,
						},
						{
							Interval: 1,
							Count:    3,
						},
					},
				},
			},
		},
	}

	stage := stageFactory(&options)

	testStage(t, c, time.UTC, stage, expected)
}

// RunTestWeekdayHistogram tests results of StoreWeekdayHistogram option.
func RunTestWeekdayHistogram(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"_date": helpers.ParseDate("2017-04-17T13:30:00+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_date": helpers.ParseDate("2017-04-18T13:30:00+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_date": helpers.ParseDate("2017-04-19T13:30:00+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
		"_date": helpers.ParseDate("2017-04-20T13:30:00+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c4"),
		"_date": helpers.ParseDate("2017-04-21T13:30:00+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c5"),
		"_date": helpers.ParseDate("2017-04-24T13:30:00+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c6"),
		"_date": helpers.ParseDate("2017-04-25T13:30:00+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c7"),
		"_date": helpers.ParseDate("2017-04-26T13:30:00+00:00"),
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreWeekdayHistogram = true

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 8,
			},
		},
		group.Result{
			Name: "_date",
			Type: analysis.Type{
				Name:  "date",
				Count: 8,
				WeekdayHistogram: &analysis.WeekdayHistogram{
					0, // sunday
					2, // monday
					2, // ...
					2,
					1,
					1,
					0,
				},
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}

// RunTestHourHistogram tests results of StoreHourHistogram option.
func RunTestHourHistogram(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"_date": helpers.ParseDate("2017-04-17T00:00:01+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_date": helpers.ParseDate("2017-04-18T23:59:59+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_date": helpers.ParseDate("2017-04-19T02:30:00+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
		"_date": helpers.ParseDate("2017-04-20T03:59:59+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c4"),
		"_date": helpers.ParseDate("2017-04-21T20:35:00+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c5"),
		"_date": helpers.ParseDate("2017-04-24T20:30:00+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c6"),
		"_date": helpers.ParseDate("2017-04-25T20:30:00+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c7"),
		"_date": helpers.ParseDate("2017-04-26T23:30:00+00:00"),
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreHourHistogram = true

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 8,
			},
		},
		group.Result{
			Name: "_date",
			Type: analysis.Type{
				Name:  "date",
				Count: 8,
				HourHistogram: &analysis.HourHistogram{
					1, // 00
					0, // 01
					1,
					1,
					0,
					0,
					0,
					0,
					0,
					0,
					0, // 10
					0, // 11
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					3, // 20
					0, // 21
					0, // 22
					2, // 23
				},
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}

// RunTestDateStatsTimezone tests results of group stage with different timezones.
func RunTestDateStatsTimezone(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"_date": helpers.ParseDate("2017-04-17T23:59:59+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_date": helpers.ParseDate("2017-04-19T12:00:00+00:00"),
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_date": helpers.ParseDate("2017-04-21T00:00:01+00:00"),
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreMinMaxAvgValue = true
	options.StoreWeekdayHistogram = true
	options.StoreHourHistogram = true
	options.ValueHistogramMaxRes = 100

	// --------------------------------------------------------------------
	// UTC timezone
	expectedUTC := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 3,
				ValueStats: &analysis.ValueStats{
					Min: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
					Max: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
				},
			},
		},
		group.Result{
			Name: "_date",
			Type: analysis.Type{
				Name:  "date",
				Count: 3,
				ValueStats: &analysis.ValueStats{
					Min: helpers.ParseDate("2017-04-17T23:59:59+00:00"),
					Max: helpers.ParseDate("2017-04-21T00:00:01+00:00"),
				},
				HourHistogram: &analysis.HourHistogram{
					1, // 00
					0, // 01
					0, // 02
					0, // 03
					0, // 04
					0, // 05
					0, // 06
					0, // 07
					0, // 08
					0, // 09
					0, // 10
					0, // 11
					1, // 12
					0, // 13
					0, // 14
					0, // 15
					0, // 16
					0, // 17
					0, // 18
					0, // 19
					0, // 20
					0, // 21
					0, // 22
					1, // 23
				},
				WeekdayHistogram: &analysis.WeekdayHistogram{
					0, // sunday
					1, // monday
					0,
					1,
					0,
					1,
					0,
				},
				ValueHistogram: &analysis.Histogram{
					Start:         helpers.ParseDate("2017-04-17T22:00:00+00:00"),
					End:           helpers.ParseDate("2017-04-21T04:00:00+00:00"),
					Range:         280800,
					Step:          7200,
					NumberOfSteps: 39,
					Intervals: analysis.Intervals{
						{
							Interval: 0,
							Count:    1,
						},
						{
							Interval: 19,
							Count:    1,
						},
						{
							Interval: 37,
							Count:    1,
						},
					},
				},
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expectedUTC)

	// --------------------------------------------------------------------
	// America/New_York timezone

	locationNY, err := time.LoadLocation("America/New_York")
	if err != nil {
		panic(err)
	}

	expectedNY := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 3,
				ValueStats: &analysis.ValueStats{
					Min: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
					Max: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
				},
			},
		},
		group.Result{
			Name: "_date",
			Type: analysis.Type{
				Name:  "date",
				Count: 3,
				ValueStats: &analysis.ValueStats{
					Min: helpers.ParseDate("2017-04-17T19:59:59-04:00").In(locationNY),
					Max: helpers.ParseDate("2017-04-20T20:00:01-04:00").In(locationNY),
				},
				HourHistogram: &analysis.HourHistogram{
					0, // 00
					0, // 01
					0, // 02
					0, // 03
					0, // 04
					0, // 05
					0, // 06
					0, // 07
					1, // 08
					0, // 09
					0, // 10
					0, // 11
					0, // 12
					0, // 13
					0, // 14
					0, // 15
					0, // 16
					0, // 17
					0, // 18
					1, // 19
					1, // 20
					0, // 21
					0, // 22
					0, // 23
				},
				WeekdayHistogram: &analysis.WeekdayHistogram{
					0, // sunday
					1, // monday
					0,
					1,
					1,
					0,
					0,
				},
				ValueHistogram: &analysis.Histogram{
					Start:         helpers.ParseDate("2017-04-17T18:00:00-04:00").In(locationNY),
					End:           helpers.ParseDate("2017-04-21T00:00:00-04:00").In(locationNY),
					Range:         280800,
					Step:          7200,
					NumberOfSteps: 39,
					Intervals: analysis.Intervals{
						{
							Interval: 0,
							Count:    1,
						},
						{
							Interval: 19,
							Count:    1,
						},
						{
							Interval: 37,
							Count:    1,
						},
					},
				},
			},
		},
	}

	testStage(t, c, locationNY, stageFactory(&options), expectedNY)
}
