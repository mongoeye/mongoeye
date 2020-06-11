// Package expandInDBCommon contains common functions for expandInDBDepth and expandInDBSeq packages.
package expandInDBCommon

import (
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"gopkg.in/mgo.v2/bson"
)

// FieldVars is an auxiliary structure that serves to build aggregation in the database.
type FieldVars struct {
	Name  interface{}
	Type  interface{}
	Value interface{}
	Level uint
}

// ProcessStringField generates operations that process string field in aggregation pipeline.
func ProcessStringField(fullName interface{}, field FieldVars, options *expand.Options) bson.M {
	m := bson.M{
		expand.BsonFieldName: fullName,
		expand.BsonFieldType: field.Type,
		expand.BsonLevel:     field.Level,
		expand.BsonNested:    nil,
	}

	if options.StoreStringLength {
		m[expand.BsonLength] = bson.M{"$strLenCP": field.Value}
	}

	if options.StoreValue {
		m[expand.BsonValue] = bson.M{
			"$substrCP": []interface{}{
				field.Value,
				0,
				options.StringMaxLength,
			},
		}
	}

	return m
}

// ProcessScalarField generates operations that process scalar field in aggregation pipeline.
func ProcessScalarField(fullName interface{}, field FieldVars, options *expand.Options) bson.M {
	m := bson.M{
		expand.BsonFieldName: fullName,
		expand.BsonFieldType: field.Type,
		expand.BsonLevel:     field.Level,
		expand.BsonNested:    nil,
	}

	if options.StoreValue {
		m[expand.BsonValue] = field.Value
	}

	return m
}
