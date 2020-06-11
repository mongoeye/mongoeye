// Package mergeInDB is the implementation of the merge stage that runs in database.
package mergeInDB

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/analysis/stages/04merge"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
)

// NewStage - MergeInDB stage factory
func NewStage(options *merge.Options) *analysis.Stage {
	return &analysis.Stage{
		PipelineFactory: func(analysisOptions *analysis.Options) *expr.Pipeline {
			// Create pipeline
			p := expr.NewPipeline()

			p.AddStage("sort", bson.M{
				analysis.BsonFieldType: 1,
			})

			p.AddStage("group", bson.M{
				analysis.BsonId: bson.M{
					merge.BsonFieldName: expr.Field(group.BsonFieldName),
				},
				merge.BsonCount: bson.M{"$sum": expr.Field(analysis.BsonCount)},
				merge.BsonTypes: bson.M{
					"$push": expr.Var("ROOT"),
				},
			})

			p.AddStage("project", bson.M{
				analysis.BsonId:     0,
				merge.BsonFieldName: expr.Field(analysis.BsonId, merge.BsonFieldName),
				merge.BsonCount:     1,
				merge.BsonTypes:     1,
			})

			p.AddStage("sort", bson.M{
				merge.BsonFieldName: 1,
			})

			return p
		},
	}
}
