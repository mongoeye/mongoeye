package expandTests

import (
	"github.com/jinzhu/copier"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/tests"
	"gopkg.in/mgo.v2/bson"
	"testing"
)

// RunTestArrayFieldMinimal tests expand stage with array field and minimal configuration.
func RunTestArrayFieldMinimal(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_array": []interface{}{
			"string",
			15,
			123.456,
			bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
		},
	})
	c.Insert(bson.M{
		"_id":    bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_array": []interface{}{},
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
			Name:  "_array",
			Type:  "array",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Type:  "string",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Type:  "int",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Type:  "double",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Type:  "objectId",
		},
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
		},
		expand.Value{
			Level: 0,
			Name:  "_array",
			Type:  "array",
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}

// RunTestArrayFieldLength tests expand stage with array field and StoreArrayLength option.
func RunTestArrayFieldLength(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_array": []interface{}{
			"string",
			15,
			123.456,
			bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
		},
	})
	c.Insert(bson.M{
		"_id":    bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_array": []interface{}{},
	})

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.StoreArrayLength = true

	expected := []interface{}{
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
		},
		expand.Value{
			Level:  0,
			Name:   "_array",
			Type:   "array",
			Length: 4,
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Type:  "string",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Type:  "int",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Type:  "double",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Type:  "objectId",
		},
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
		},
		expand.Value{
			Level:  0,
			Name:   "_array",
			Type:   "array",
			Length: 0,
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}

// RunTestArrayFieldValue tests expand stage with array field and StoreValue option.
func RunTestArrayFieldValue(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_array": []interface{}{
			"string",
			15,
			123.456,
			bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
		},
	})
	c.Insert(bson.M{
		"_id":    bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		"_array": []interface{}{},
	})

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.StoreValue = true

	expected := []interface{}{
		expand.Value{
			Level: 0,
			Name:  "_id",
			Value: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
			Type:  "objectId",
		},
		expand.Value{
			Level: 0,
			Name:  "_array",
			Value: []interface{}{
				"string",
				15,
				123.456,
				bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
			},
			Type: "array",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Value: "string",
			Type:  "string",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Value: 15,
			Type:  "int",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Value: 123.456,
			Type:  "double",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Value: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c3"),
			Type:  "objectId",
		},
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
			Value: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c1"),
		},
		expand.Value{
			Level: 0,
			Name:  "_array",
			Value: []interface{}{},
			Type:  "array",
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}

// RunTestArrayFieldArrayMaxLength tests expand stage with array field and ArrayMaxLength option.
func RunTestArrayFieldArrayMaxLength(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id":    bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_array": []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	})

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.StoreValue = true
	options.StoreArrayLength = true
	options.ArrayMaxLength = 3

	expected := []interface{}{
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
			Value: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		},
		expand.Value{
			Level: 0,
			Name:  "_array",
			Value: []interface{}{
				1, 2, 3,
			},
			Type:   "array",
			Length: 10,
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Value: 1,
			Type:  "int",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Value: 2,
			Type:  "int",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Value: 3,
			Type:  "int",
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}

// RunTestArrayFieldMaxDepth tests expand stage with array field and MaxDepth option.
func RunTestArrayFieldMaxDepth(t *testing.T, stageFactory expand.StageFactory) {
	c := tests.SetupTestCol()
	defer tests.TearDownTestCol(c)

	c.Insert(bson.M{
		"_id": bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		"_array": []interface{}{
			1, 2, 3, 4,
			[]interface{}{
				5, 6, 7,
				[]interface{}{
					8, 9,
					[]interface{}{
						10, 11, 12,
						[]interface{}{
							13,
						},
					},
				},
			},
		},
	})

	options := expand.Options{}
	copier.Copy(&options, &testOptions)
	options.StoreValue = true
	options.StoreArrayLength = true
	options.MaxDepth = 2

	expected := []interface{}{
		expand.Value{
			Level: 0,
			Name:  "_id",
			Type:  "objectId",
			Value: bson.ObjectIdHex("58e20d849d3ae7e1f8eac9c2"),
		},
		expand.Value{
			Level: 0,
			Name:  "_array",
			Value: []interface{}{
				1, 2, 3, 4,
				[]interface{}{
					5, 6, 7,
					[]interface{}{
						8, 9,
						[]interface{}{
							10, 11, 12,
							[]interface{}{
								13,
							},
						},
					},
				},
			},
			Type:   "array",
			Length: 5,
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Value: 1,
			Type:  "int",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Value: 2,
			Type:  "int",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Value: 3,
			Type:  "int",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Value: 4,
			Type:  "int",
		},
		expand.Value{
			Level: 1,
			Name:  "_array.[]",
			Value: []interface{}{
				5, 6, 7,
				[]interface{}{
					8, 9,
					[]interface{}{
						10, 11, 12,
						[]interface{}{
							13,
						},
					},
				},
			},
			Type:   "array",
			Length: 4,
		},
		expand.Value{
			Level: 2,
			Name:  "_array.[].[]",
			Value: 5,
			Type:  "int",
		},
		expand.Value{
			Level: 2,
			Name:  "_array.[].[]",
			Value: 6,
			Type:  "int",
		},
		expand.Value{
			Level: 2,
			Name:  "_array.[].[]",
			Value: 7,
			Type:  "int",
		},
		expand.Value{
			Level: 2,
			Name:  "_array.[].[]",
			Value: []interface{}{
				8, 9,
				[]interface{}{
					10, 11, 12,
					[]interface{}{
						13,
					},
				},
			},
			Type:   "array",
			Length: 3,
		},
	}

	testStage(t, c, stageFactory(&options), expected)
}
