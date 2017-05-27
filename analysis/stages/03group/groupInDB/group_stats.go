package groupInDB

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
)

// GroupStats computes stats of values and lengths.
// Example results:
//	[
//		{
//			FIELD_NAME: "name"
//			FIELD_TYPE: "string",
//			COUNT: 5
//			VALUE_HISTOGRAM: ...
//			...
//		},
//		...
//	]
func GroupStats(p *expr.Pipeline, groupOptions *group.Options) {
	// Group stats
	statType := expr.Field(analysis.BsonId, statType)
	sw := expr.Switch()

	// Base
	sw.AddBranch(
		expr.Eq(statType, baseStats),
		bson.M{
			group.BsonFieldName:    expr.Field(analysis.BsonId, group.BsonFieldName),
			analysis.BsonFieldType: expr.Field(analysis.BsonId, analysis.BsonFieldType),
			analysis.BsonCount:     expr.Field(analysis.BsonCount),
		},
	)

	// Value extremes
	sw.AddBranch(
		expr.Eq(statType, valueExtremes),
		bson.M{
			analysis.BsonValueExtremes: bson.M{
				analysis.BsonMinValue: expr.Cond(
					expr.Eq(expr.Field(analysis.BsonId, analysis.BsonFieldType), "bool"),
					expr.Eq(expr.Field(analysis.BsonMinValue), 1),
					expr.Field(analysis.BsonMinValue),
				),
				analysis.BsonMaxValue: expr.Cond(
					expr.Eq(expr.Field(analysis.BsonId, analysis.BsonFieldType), "bool"),
					expr.Eq(expr.Field(analysis.BsonMaxValue), 1),
					expr.Field(analysis.BsonMaxValue),
				),
				analysis.BsonAvgValue: expr.Field(analysis.BsonAvgValue),
			},
		},
	)

	// Length extremes
	sw.AddBranch(
		expr.Eq(statType, lengthExtremes),
		bson.M{
			analysis.BsonLengthExtremes: bson.M{
				analysis.BsonMinLength: expr.Field(analysis.BsonMinLength),
				analysis.BsonMaxLength: expr.Field(analysis.BsonMaxLength),
				analysis.BsonAvgLength: expr.Field(analysis.BsonAvgLength),
			},
		},
	)

	// Top and bottom values
	sw.AddBranch(
		expr.Eq(statType, valueFreqStats),
		bson.M{
			analysis.BsonCountUnique:   expr.Field(analysis.BsonCountUnique),
			analysis.BsonTopNValues:    expr.Field(analysis.BsonTopNValues),
			analysis.BsonBottomNValues: expr.Field(analysis.BsonBottomNValues),
		},
	)

	// ValuesHistogram
	sw.AddBranch(
		expr.Eq(statType, valueHistogram),
		bson.M{
			analysis.BsonValueHistogram: bson.M{
				analysis.BsonHistogramStart: expr.Cond(
					expr.Eq(expr.Field(analysis.BsonId, analysis.BsonFieldType), "date"),
					expr.TimestampToDate(expr.Field(analysis.BsonHistogramStart)),
					expr.Field(analysis.BsonHistogramStart),
				),
				analysis.BsonHistogramEnd: expr.Cond(
					expr.Eq(expr.Field(analysis.BsonId, analysis.BsonFieldType), "date"),
					expr.TimestampToDate(expr.Field(analysis.BsonHistogramEnd)),
					expr.Field(analysis.BsonHistogramEnd),
				),
				analysis.BsonHistogramRange:      expr.Field(analysis.BsonHistogramRange),
				analysis.BsonHistogramStep:       expr.Field(analysis.BsonHistogramStep),
				analysis.BsonHistogramNumOfSteps: expr.Field(analysis.BsonHistogramNumOfSteps),
				analysis.BsonHistogramIntervals:  expr.Field(analysis.BsonHistogramIntervals),
			},
		},
	)

	// LengthsHistogram
	sw.AddBranch(
		expr.Eq(statType, lengthHistogram),
		bson.M{
			analysis.BsonLengthHistogram: bson.M{
				analysis.BsonHistogramStart:      expr.Field(analysis.BsonHistogramStart),
				analysis.BsonHistogramEnd:        expr.Field(analysis.BsonHistogramEnd),
				analysis.BsonHistogramRange:      expr.Field(analysis.BsonHistogramRange),
				analysis.BsonHistogramStep:       expr.Field(analysis.BsonHistogramStep),
				analysis.BsonHistogramNumOfSteps: expr.Field(analysis.BsonHistogramNumOfSteps),
				analysis.BsonHistogramIntervals:  expr.Field(analysis.BsonHistogramIntervals),
			},
		},
	)

	// DateWeekdayHistogram
	sw.AddBranch(
		expr.Eq(statType, weekdayHistogram),
		bson.M{
			analysis.BsonWeekdayHistogram: bson.M{
				analysis.BsonHistogramIntervals: expr.Field(analysis.BsonHistogramIntervals),
			},
		},
	)

	// DateHourHistogram
	sw.AddBranch(
		expr.Eq(statType, hourHistogram),
		bson.M{
			analysis.BsonHourHistogram: bson.M{
				analysis.BsonHistogramIntervals: expr.Field(analysis.BsonHistogramIntervals),
			},
		},
	)

	p.AddStage("group", bson.M{
		analysis.BsonId: bson.M{
			group.BsonFieldName:    expr.Field(analysis.BsonId + "." + group.BsonFieldName),
			analysis.BsonFieldType: expr.Field(analysis.BsonId + "." + analysis.BsonFieldType),
		},
		stats: bson.M{"$push": sw.Bson()},
	})

	p.AddStage("replaceRoot", bson.M{
		"newRoot": expr.MergeObjects(expr.Field(stats)),
	})
}
