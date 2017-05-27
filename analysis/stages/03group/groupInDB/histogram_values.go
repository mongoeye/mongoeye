package groupInDB

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
)

// ValuesHistogram returns lengths histogram calculation pipeline.
func ValuesHistogram(options *group.Options) *expr.Pipeline {
	p := expr.NewPipeline()

	typeField := analysis.BsonId + "." + analysis.BsonFieldType
	p.AddStage("match", bson.M{
		typeField: bson.M{"$in": group.ValueHistogramTypes},
	})

	generateHistogram(p, analysis.BsonValueHistogram, analysis.BsonMinValue, analysis.BsonMaxValue, expand.BsonValue, expr.Field(typeField), options.ValueHistogramMaxRes)

	return p
}
