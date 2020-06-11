package expandTests

import (
	"github.com/jinzhu/copier"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/tests"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

// RunTestStringFieldMinimal tests string field with minimal configuration.
func RunTestStringFieldMinimal(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id":    bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"String": "abc",
	})
	c.Insert(bson.M{
		"_id":    bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"String": "",
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
			Name:  "String",
			Type:  "string",
		},
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
		},
		expand.Value{
			Level: 0,
			Name:  "String",
			Type:  "string",
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}

// RunTestStringFieldValue tests string field - StoreValue option.
func RunTestStringFieldValue(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id":    bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"String": "abc",
	})
	c.Insert(bson.M{
		"_id":    bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"String": "",
	})

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.StoreValue = true

	expected := []interface{}{
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
			Value: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		},
		expand.Value{
			Level: 0,
			Name:  "String",
			Type:  "string",
			Value: "abc",
		},
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
			Value: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		},
		expand.Value{
			Level: 0,
			Name:  "String",
			Type:  "string",
			Value: "",
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}

// RunTestStringFieldLength tests string field - StoreLength option.
func RunTestStringFieldLength(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id":    bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"String": "Šašo",
	})
	c.Insert(bson.M{
		"_id":    bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"String": "",
	})

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.StoreStringLength = true

	expected := []interface{}{
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
		},
		expand.Value{
			Level:  0,
			Name:   "String",
			Type:   "string",
			Length: 4,
		},
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
		},
		expand.Value{
			Level:  0,
			Name:   "String",
			Type:   "string",
			Length: 0,
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}

// RunTestStringFieldMaxLength tests string field - StringMaxLength option.
func RunTestStringFieldMaxLength(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id":    bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		"String": "Šašo",
	})
	c.Insert(bson.M{
		"_id":    bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"String": "abcdefghijklmnoprst",
	})

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.StringMaxLength = 6
	options.StoreStringLength = true
	options.StoreValue = true

	expected := []interface{}{
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
			Value: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c0"),
		},
		expand.Value{
			Level:  0,
			Name:   "String",
			Type:   "string",
			Length: 4,
			Value:  "Šašo",
		},
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
			Value: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		},
		expand.Value{
			Level:  0,
			Name:   "String",
			Type:   "string",
			Length: 19,
			Value:  "abcdef",
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}

// RunTestStringFieldAll tests string field - all options.
func RunTestStringFieldAll(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id":    bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"String": "abc",
	})
	c.Insert(bson.M{
		"_id":    bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"String": "",
	})

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.StoreValue = true
	options.StoreStringLength = true

	expected := []interface{}{
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
			Value: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		},
		expand.Value{
			Level:  0,
			Name:   "String",
			Type:   "string",
			Value:  "abc",
			Length: 3,
		},
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
			Value: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		},
		expand.Value{
			Level:  0,
			Name:   "String",
			Type:   "string",
			Value:  "",
			Length: 0,
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}
