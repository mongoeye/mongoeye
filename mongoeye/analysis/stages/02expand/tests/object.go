package expandTests

import (
	"github.com/jinzhu/copier"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/tests"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

// RunTestObjectFieldMinimal tests expand stage with minimal configuration.
func RunTestObjectFieldMinimal(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_subDocument": bson.M{
			"Null":   nil,
			"String": "string",
			"Int":    15,
			"Double": 123.456,
			"Id":     bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
		},
	})
	c.Insert(bson.M{
		"_id":          bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_subDocument": bson.M{},
	})

	options := expand.Options{}
	copier.Copy(&options, &testOptions)

	expected := []interface{}{
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
		},
		expand.Value{
			Level: 0,
			Name:  "_subDocument",
			Type:  "object",
		},
		expand.Value{
			Level: 1,
			Name:  "_subDocument.Null",
			Type:  "null",
		},
		expand.Value{
			Level: 1,
			Name:  "_subDocument.String",
			Type:  "string",
		},
		expand.Value{
			Level: 1,
			Name:  "_subDocument.Int",
			Type:  "int",
		},
		expand.Value{
			Level: 1,
			Name:  "_subDocument.Double",
			Type:  "double",
		},
		expand.Value{
			Level: 1,
			Name:  "_subDocument.Id",
			Type:  "objectId",
		},
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
		},
		expand.Value{
			Level: 0,
			Name:  "_subDocument",
			Type:  "object",
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}

// RunTestObjectFieldLength tests expand stage with StoreObjectLength option.
func RunTestObjectFieldLength(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_subDocument": bson.M{
			"Null":   nil,
			"String": "string",
			"Int":    15,
			"Double": 123.456,
			"Id":     bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
		},
	})
	c.Insert(bson.M{
		"_id":          bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_subDocument": bson.M{},
	})

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.StoreObjectLength = true

	expected := []interface{}{
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
		},
		expand.Value{
			Level:  0,
			Name:   "_subDocument",
			Type:   "object",
			Length: 5,
		},
		expand.Value{
			Level: 1,
			Name:  "_subDocument.Null",
			Type:  "null",
		},
		expand.Value{
			Level: 1,
			Name:  "_subDocument.String",
			Type:  "string",
		},
		expand.Value{
			Level: 1,
			Name:  "_subDocument.Int",
			Type:  "int",
		},
		expand.Value{
			Level: 1,
			Name:  "_subDocument.Double",
			Type:  "double",
		},
		expand.Value{
			Level: 1,
			Name:  "_subDocument.Id",
			Type:  "objectId",
		},
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
		},
		expand.Value{
			Level:  0,
			Name:   "_subDocument",
			Type:   "object",
			Length: 0,
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}

// RunTestObjectFieldMaxDepth tests expand stage with MaxDepth option.
func RunTestObjectFieldMaxDepth(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_subDocument": bson.M{
			"Level1": bson.M{
				"Level2": bson.M{
					"Level3": bson.M{
						"Level4": bson.M{
							"Level5": bson.M{
								"Int": 10,
							},
						},
					},
				},
			},
		},
	})
	c.Insert(bson.M{
		"_id":          bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_subDocument": bson.M{},
	})

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.StoreObjectLength = true
	options.MaxDepth = 2

	expected := []interface{}{
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
		},
		expand.Value{
			Level:  0,
			Name:   "_subDocument",
			Type:   "object",
			Length: 1,
		},
		expand.Value{
			Level:  1,
			Name:   "_subDocument.Level1",
			Type:   "object",
			Length: 1,
		},
		expand.Value{
			Level:  2,
			Name:   "_subDocument.Level1.Level2",
			Type:   "object",
			Length: 1,
		},
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
		},
		expand.Value{
			Level:  0,
			Name:   "_subDocument",
			Type:   "object",
			Length: 0,
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}

// RunTestObjectFieldValue tests expand stage with StoreValue option.
func RunTestObjectFieldValue(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_object": bson.M{
			"key1": "value1",
			"key2": "value2",
		},
	})
	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_object": bson.M{
			"key3": "value3",
		},
	})

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.StoreValue = true
	options.MaxDepth = 0

	expected := []interface{}{
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
			Value: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		},
		expand.Value{
			Level: 0,
			Name:  "_object",
			Type:  "object",
			Value: bson.M{
				"key1": "value1",
				"key2": "value2",
			},
		},
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
			Value: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		},
		expand.Value{
			Level: 0,
			Name:  "_object",
			Type:  "object",
			Value: bson.M{
				"key3": "value3",
			},
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}
