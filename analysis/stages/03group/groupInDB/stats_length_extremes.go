package groupInDB

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
)

// LengthStatsComputation adds length extremes computation to aggregation pipeline
// Example results:
//	[
//		{
//			ID: {
//				FIELD_NAME: "name"
//				FIELD_TYPE: "string",
//				STAT_TYPE: LENGTH_EXTREMES,
//			}
//			MIN_LENGTH: 2,
//			MAX_LENGTH: 5,
//			AVG_LENGTH: 2.45,
//			...
//		},
//		...
//	]
func LengthStatsComputation(options *group.Options) *expr.Pipeline {
	p := expr.NewPipeline()

	p.AddStage("match", bson.M{
		(analysis.BsonId + "." + analysis.BsonFieldType): bson.M{"$in": group.StoreLengthTypes},
	})

	project := bson.M{
		analysis.BsonId: bson.M{
			group.BsonFieldName:    expr.Field(analysis.BsonId, group.BsonFieldName),
			analysis.BsonFieldType: expr.Field(analysis.BsonId, analysis.BsonFieldType),
			statType:               lengthStats,
		},
		analysis.BsonMinLength: 1,
		analysis.BsonMaxLength: 1,
		analysis.BsonAvgLength: 1,
	}

	p.AddStage("project", project)

	return p
}
