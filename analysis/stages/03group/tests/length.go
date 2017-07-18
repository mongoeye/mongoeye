package groupTests

import (
	"github.com/jinzhu/copier"
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

// RunTestStringLengthMinMaxAvg tests group stage with StoreMinMaxAvgLength option (string field).
func RunTestStringLengthMinMaxAvg(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"field": "",
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"field": "abcde",
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"field": "abcdeabcde",
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreMinMaxAvgLength = true

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 3,
			},
		},
		group.Result{
			Name: "field",
			Type: analysis.Type{
				Name:  "string",
				Count: 3,
				LengthStats: &analysis.LengthStats{
					Min: 0,
					Max: 10,
					Avg: 5,
				},
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}

// RunTestArrayLengthMinMaxAvg tests group stage with StoreMinMaxAvgLength option (array field).
func RunTestArrayLengthMinMaxAvg(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"field": []interface{}{},
	})
	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"field": []interface{}{
			12,
		},
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"field": []interface{}{},
	})
	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
		"field": []interface{}{
			"abc",
			12,
			13,
			14,
			"cde",
		},
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreMinMaxAvgLength = true

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 4,
			},
		},
		group.Result{
			Name: "field",
			Type: analysis.Type{
				Name:  "array",
				Count: 4,
				LengthStats: &analysis.LengthStats{
					Min: 0,
					Max: 5,
					Avg: 1.5,
				},
			},
		},
		group.Result{
			Name: "field.[]",
			Type: analysis.Type{
				Name:  "int",
				Count: 4,
			},
		},
		group.Result{
			Name: "field.[]",
			Type: analysis.Type{
				Name:  "string",
				Count: 2,
				LengthStats: &analysis.LengthStats{
					Min: 3,
					Max: 3,
					Avg: 3,
				},
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}

// RunTestObjectLengthMinMaxAvg tests group stage with StoreMinMaxAvgLength option (object field).
func RunTestObjectLengthMinMaxAvg(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"field": bson.M{},
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"field": bson.M{"f1": 12},
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"field": bson.M{},
	})
	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
		"field": bson.M{
			"f1": 12,
			"f2": "abc",
			"f3": 13,
			"f4": 14,
			"f5": "cde",
		},
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)
	options.StoreMinMaxAvgLength = true

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 4,
			},
		},
		group.Result{
			Name: "field",
			Type: analysis.Type{
				Name:  "object",
				Count: 4,
				LengthStats: &analysis.LengthStats{
					Min: 0,
					Max: 5,
					Avg: 1.5,
				},
			},
		},
		group.Result{
			Name: "field.f1",
			Type: analysis.Type{
				Name:  "int",
				Count: 2,
			},
		},
		group.Result{
			Name: "field.f2",
			Type: analysis.Type{
				Name:  "string",
				Count: 1,
				LengthStats: &analysis.LengthStats{
					Min: 3,
					Max: 3,
					Avg: 3,
				},
			},
		},
		group.Result{
			Name: "field.f3",
			Type: analysis.Type{
				Name:  "int",
				Count: 1,
			},
		},
		group.Result{
			Name: "field.f4",
			Type: analysis.Type{
				Name:  "int",
				Count: 1,
			},
		},
		group.Result{
			Name: "field.f5",
			Type: analysis.Type{
				Name:  "string",
				Count: 1,
				LengthStats: &analysis.LengthStats{
					Min: 3,
					Max: 3,
					Avg: 3,
				},
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}
