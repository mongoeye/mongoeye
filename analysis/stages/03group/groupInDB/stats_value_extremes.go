package groupInDB

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
)

// ValueExtremesComputation generates pipeline to compute min, max and avg value.
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
func ValueExtremesComputation(options *group.Options) *expr.Pipeline {
	p := expr.NewPipeline()

	p.AddStage("match", bson.M{
		(analysis.BsonId + "." + analysis.BsonFieldType): bson.M{"$in": group.StoreMinMaxValueTypes},
	})

	project := bson.M{
		analysis.BsonId: bson.M{
			statType:               valueExtremes,
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
