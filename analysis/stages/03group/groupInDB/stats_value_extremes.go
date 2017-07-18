package groupInDB

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
)

// ValueStatsComputation generates pipeline to compute min, max and avg value.
// Example results:
//	[
//		{
//			ID: {
//				FIELD_NAME: "name"
//				FIELD_TYPE: "string",
//				STAT_TYPE: VALUE_EXTREMES,
//			}
//			MIN_VALUE: "abc",
//			MAX_VALUE: "NKLXYZ",
//			AVG_VALUE: nil,
//			...
//		},
//		...
//	]
func ValueStatsComputation(options *group.Options) *expr.Pipeline {
	p := expr.NewPipeline()

	allowedTypes := group.StoreMinMaxValueTypes[:]
	// Allows objectId to be processed as a date,
	// value is converted in "prepareFields" function
	if options.ProcessObjectIdAsDate {
		allowedTypes = append(allowedTypes, "objectId")
	}

	p.AddStage("match", bson.M{
		(analysis.BsonId + "." + analysis.BsonFieldType): bson.M{"$in": allowedTypes},
	})

	project := bson.M{
		analysis.BsonId: bson.M{
			statType:               valueStats,
			group.BsonFieldName:    expr.Field(analysis.BsonId, group.BsonFieldName),
			analysis.BsonFieldType: expr.Field(analysis.BsonId, analysis.BsonFieldType),
		},
		analysis.BsonMinValue: 1,
		analysis.BsonMaxValue: 1,
		analysis.BsonAvgValue: 1,
	}

	p.AddStage("project", project)

	return p
}
