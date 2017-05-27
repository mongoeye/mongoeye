package groupInDB

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
)

// BaseComputation generates operations for compute base statistics.
// Example results:
//	[
//		{
//			ID: {
//				FIELD_NAME: "name"
//				FIELD_TYPE: "string",
//				STAT_TYPE: BASE_STATS,
//			}
//			COUNT: 5
//			...
//		},
//		...
//	]
func BaseComputation(options *group.Options) *expr.Pipeline {
	p := expr.NewPipeline()

	project := bson.M{
		analysis.BsonId: bson.M{
			group.BsonFieldName:    expr.Field(analysis.BsonId, group.BsonFieldName),
			analysis.BsonFieldType: expr.Field(analysis.BsonId, analysis.BsonFieldType),
			statType:               baseStats,
		},
		analysis.BsonCount: 1,
	}

	p.AddStage("project", project)

	return p
}
