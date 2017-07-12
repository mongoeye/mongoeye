package groupInDB

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
)

// LengthsHistogram adds lengths histogram calculation to aggregation pipeline.
func LengthsHistogram(options *group.Options) *expr.Pipeline {
	p := expr.NewPipeline()

	fieldType := analysis.BsonId + "." + analysis.BsonFieldType
	p.AddStage("match", bson.M{
		fieldType: bson.M{"$in": group.LengthHistogramTypes},
	})

	generateHistogram(p, analysis.BsonLengthHistogram, analysis.BsonMinLength, analysis.BsonMaxLength, expand.BsonLength, "int", options.LengthHistogramMaxRes, false)

	return p
}
