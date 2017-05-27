package groupInDB

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
	"time"
)

// DateHourHistogram returns hour histogram histogram calculation pipeline.
func DateHourHistogram(location *time.Location) *expr.Pipeline {
	p := expr.NewPipeline()

	nameField := expr.Field(analysis.BsonId, group.BsonFieldName)
	typeField := expr.Field(analysis.BsonId, analysis.BsonFieldType)

	p.AddStage("match", bson.M{
		(analysis.BsonId + "." + analysis.BsonFieldType): "date",
	})

	p.AddStage("project", bson.M{
		analysis.BsonId: 1,
		bsonAllValues: expr.Map(
			expr.Field(bsonAllValues),
			"i",
			expr.Hour(ConvertDateToTimezone(expr.Var("i", expand.BsonValue), location)),
		),
	})

	p.AddStage("unwind", expr.Field(bsonAllValues))

	p.AddStage("group", bson.M{
		analysis.BsonId: bson.M{
			group.BsonFieldName:        nameField,
			analysis.BsonFieldType:     typeField,
			analysis.BsonIntervalValue: expr.Field(bsonAllValues),
		},
		analysis.BsonCount: bson.M{"$sum": 1},
	})

	p.AddStage("sort", bson.M{
		analysis.BsonId + "." + analysis.BsonIntervalValue: 1,
	})

	p.AddStage("group", bson.M{
		analysis.BsonId: bson.M{
			group.BsonFieldName:    nameField,
			analysis.BsonFieldType: typeField,
			statType:               hourHistogram,
		},
		analysis.BsonHistogramIntervals: bson.M{
			"$push": bson.M{
				analysis.BsonIntervalValue: expr.Field(analysis.BsonId, analysis.BsonIntervalValue),
				analysis.BsonIntervalCount: expr.Field(analysis.BsonIntervalCount),
			},
		},
	})

	return p
}
