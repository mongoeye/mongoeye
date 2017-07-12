package groupInDB

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
)

// ValueFreqComputation generates pipeline that compute most and least frequent values.
// Example results:
//	[
//		{
//			ID: {
//				FIELD_NAME: "name"
//				FIELD_TYPE: "string",
//				STAT_TYPE: VALUE_FREQ_STATS,
//			}
//			COUNT_UNIQUE: 4,
//			TOP_N_VALUES: ...
//			BOTTOM_N_VALUES: ...
//		},
//		...
//	]
func ValueFreqComputation(options *group.Options) *expr.Pipeline {
	p := expr.NewPipeline()

	nameField := expr.Field(analysis.BsonId, group.BsonFieldName)
	typeField := expr.Field(analysis.BsonId, analysis.BsonFieldType)

	allowedTypes := group.TopBottomValuesTypes[:]
	// Allows objectId to be processed as a date,
	// value is converted in "prepareFields" function
	if options.ProcessObjectIdAsDate {
		allowedTypes = append(allowedTypes, "objectId")
	}

	p.AddStage("match", bson.M{
		(analysis.BsonId + "." + analysis.BsonFieldType): bson.M{"$in": allowedTypes},
	})

	//if options.MaxItemsForFreqAnalysis > 0 {
	//	p.AddStage("project", bson.M{
	//		group.ALL_VALUES: expr.Slice("$" + group.ALL_VALUES, options.MaxItemsForFreqAnalysis),
	//	})
	//}

	// Calculate frequency
	p.AddStage("unwind", expr.Field(bsonAllValues))

	// Group values
	groupValues := bson.M{
		analysis.BsonId: bson.M{
			group.BsonFieldName:         nameField,
			analysis.BsonFieldType:      typeField,
			analysis.BsonValueFreqValue: expr.Field(bsonAllValues, expand.BsonValue),
		},
		analysis.BsonValueFreqCount: bson.M{"$sum": 1},
	}
	p.AddStage("group", groupValues)

	// Sort
	p.AddStage("sort", bson.M{(analysis.BsonId + "." + analysis.BsonValueFreqValue): 1})
	p.AddStage("sort", bson.M{analysis.BsonValueFreqCount: -1})

	// Group types
	groupTypes := bson.M{
		analysis.BsonId: bson.M{
			group.BsonFieldName:    nameField,
			analysis.BsonFieldType: typeField,
		},
		bsonValueFreq: bson.M{
			"$push": bson.M{
				analysis.BsonValueFreqValue: expr.Field(analysis.BsonId, analysis.BsonValueFreqValue),
				analysis.BsonValueFreqCount: expr.Field(analysis.BsonValueFreqCount),
			},
		},
	}

	// Store count of unique
	if options.StoreCountOfUnique {
		groupTypes[analysis.BsonCountUnique] = bson.M{"$sum": 1}
	}

	p.AddStage("group", groupTypes)

	// Project
	project := bson.M{
		analysis.BsonId: bson.M{
			group.BsonFieldName:    nameField,
			analysis.BsonFieldType: typeField,
			statType:               valueFreqStats,
		},
		analysis.BsonCountUnique: 1,
	}

	if options.StoreTopNValues > 0 {
		project[analysis.BsonTopNValues] = expr.Slice(expr.Field(bsonValueFreq), int(options.StoreTopNValues))
	}

	if options.StoreBottomNValues > 0 {
		project[analysis.BsonBottomNValues] = expr.Slice(expr.Field(bsonValueFreq), -int(options.StoreBottomNValues))
	}

	p.AddStage("project", project)

	return p
}
