package groupTests

import (
	"github.com/jinzhu/copier"
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

// RunTestMinimal tests group stage with minimal configuration.
func RunTestMinimal(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"f1":  5,
		"f2":  "šašo",
	})
	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"f1":  10,
		"f2":  20,
		"f3":  "xyz",
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)

	expected := []interface{}{
		group.Result{
			Name: "_id",
			Type: analysis.Type{
				Name:  "objectId",
				Count: 2,
			},
		},
		group.Result{
			Name: "f1",
			Type: analysis.Type{
				Name:  "int",
				Count: 2,
			},
		},
		group.Result{
			Name: "f2",
			Type: analysis.Type{
				Name:  "int",
				Count: 1,
			},
		},
		group.Result{
			Name: "f2",
			Type: analysis.Type{
				Name:  "string",
				Count: 1,
			},
		},
		group.Result{
			Name: "f3",
			Type: analysis.Type{
				Name:  "string",
				Count: 1,
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}

// RunTestDifferentTypesInField tests group of two different types with same field name.
func RunTestDifferentTypesInField(t *testing.T, stageFactory group.StageFactory) {
	c := setup()
	defer tearDown(c)

	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"field": 123.456,
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"field": 456.789,
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"field": "string",
	})
	c.Insert(bson.M{
		"_id":   bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
		"field": 123,
	})

	options := group.Options{}
	copier.Copy(&options, &testGroupOptions)

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
				Name:  "double",
				Count: 2,
			},
		},
		group.Result{
			Name: "field",
			Type: analysis.Type{
				Name:  "string",
				Count: 1,
			},
		},
		group.Result{
			Name: "field",
			Type: analysis.Type{
				Name:  "int",
				Count: 1,
			},
		},
	}

	testStage(t, c, time.UTC, stageFactory(&options), expected)
}
