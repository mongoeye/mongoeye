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

	allowedTypes := group.ValueHistogramTypes[:]
	// Allows objectId to be processed as a date,
	// value is converted in "prepareFields" function
	if options.ProcessObjectIdAsDate {
		allowedTypes = append(allowedTypes, "objectId")
	}

	typeField := analysis.BsonId + "." + analysis.BsonFieldType
	p.AddStage("match", bson.M{
		typeField: bson.M{"$in": allowedTypes},
	})

	generateHistogram(p, analysis.BsonValueHistogram, analysis.BsonMinValue, analysis.BsonMaxValue, expand.BsonValue, expr.Field(typeField), options.ValueHistogramMaxRes, options.ProcessObjectIdAsDate)

	return p
}
