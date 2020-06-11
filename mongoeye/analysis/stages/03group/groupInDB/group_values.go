package groupInDB

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
)

// GroupValues from expand stage by name and type.
// Example results:
//	[
//		{
//			FIELD_NAME: "name"
//			FIELD_TYPE: "string",
//                      ALL_VALUES: [
//				{
//					VALUE: "Michael",
//					LENGTH: 7,
//				},
//				{
//					VALUE: "Mike",
//					LENGTH: 4,
//				},
//                              ...
//                      ]
//		},
//		...
//	]
func GroupValues(p *expr.Pipeline, options *group.Options) {
	prepareFields(p, options)

	fields := bson.M{
		analysis.BsonId: bson.M{
			group.BsonFieldName:    expr.Field(expand.BsonFieldName),
			analysis.BsonFieldType: expr.Field(expand.BsonFieldType),
		},
		analysis.BsonCount: bson.M{"$sum": 1},
	}

	push := bson.M{}

	// Store value
	if options.StoreWeekdayHistogram ||
		options.StoreHourHistogram ||
		options.StoreCountOfUnique ||
		options.StoreMostFrequent > 0 ||
		options.StoreLeastFrequent > 0 ||
		options.ValueHistogramMaxRes > 0 {

		push[expand.BsonValue] = expr.Field(expand.BsonValue)
	}

	// Calculate value extremes
	if options.StoreMinMaxAvgValue ||
		options.ValueHistogramMaxRes > 0 {

		fields[analysis.BsonMinValue] = bson.M{"$min": expr.Field(expand.BsonValue)}
		fields[analysis.BsonMaxValue] = bson.M{"$max": expr.Field(expand.BsonValue)}
		fields[analysis.BsonAvgValue] = bson.M{"$avg": expr.Field(expand.BsonValue)}
	}

	// Store length
	if options.LengthHistogramMaxRes > 0 {
		push[expand.BsonLength] = expr.Field(expand.BsonLength)
	}

	// Calculate length extremes
	if options.StoreMinMaxAvgLength ||
		options.LengthHistogramMaxRes > 0 {

		fields[analysis.BsonMinLength] = bson.M{"$min": expr.Field(expand.BsonLength)}
		fields[analysis.BsonMaxLength] = bson.M{"$max": expr.Field(expand.BsonLength)}
		fields[analysis.BsonAvgLength] = bson.M{"$avg": expr.Field(expand.BsonLength)}
	}

	// Store value or length?
	if len(push) > 0 {
		fields[bsonAllValues] = bson.M{"$push": push}
	}

	p.AddStage("group", fields)
}

// Prepare fields for grouping:
//  - converts the values to the desired format
//  - deletes fields that will no longer be needed according to group.Options.
//
// Example results:
//	[
//		{
//			FIELD_NAME: "name",
//			FIELD_TYPE: "string",
//			VALUE: "Michael",
//			LENGTH: 7,
//		}
//		...
//	]
func prepareFields(p *expr.Pipeline, options *group.Options) {
	var valueProject interface{}
	if options.StoreMinMaxAvgValue ||
		options.StoreWeekdayHistogram ||
		options.StoreHourHistogram ||
		options.StoreCountOfUnique ||
		options.StoreMostFrequent > 0 ||
		options.StoreLeastFrequent > 0 ||
		options.ValueHistogramMaxRes > 0 {

		typeSw := expr.Switch()
		typeSw.SetDefault(expr.Field(expand.BsonValue))

		// Convert boolean to 1 or 0 values for average calculation
		typeSw.AddBranch(
			expr.Eq(expr.Field(expand.BsonFieldType), "bool"),
			expr.Cond(
				expr.Eq(expr.Field(expand.BsonValue), true),
				1,
				0,
			),
		)

		// Convert objectId to date
		if options.ProcessObjectIdAsDate {
			typeSw.AddBranch(
				expr.Eq(expr.Field(expand.BsonFieldType), "objectId"),
				expr.ObjectIdToDate(expr.Field(expand.BsonValue)),
			)
		}

		valueProject = expr.Cond(
			expr.In(expr.Field(expand.BsonFieldType), group.StoreValueTypes),
			typeSw.Bson(),
			expr.Var("REMOVE"), // remove if no longer needed
		)
	} else {
		valueProject = expr.Var("REMOVE")
	}

	var lengthProject interface{}
	if options.StoreMinMaxAvgLength ||
		options.LengthHistogramMaxRes > 0 {

		lengthProject = expr.Cond(
			expr.In(expr.Field(expand.BsonFieldType), group.StoreLengthTypes),
			expr.Field(expand.BsonLength),
			expr.Var("REMOVE"), // remove if no longer needed
		)
	} else {
		lengthProject = expr.Var("REMOVE") // remove if no longer needed
	}

	p.AddStage("project", bson.M{
		expand.BsonFieldName: 1,
		expand.BsonFieldType: 1,
		expand.BsonValue:     valueProject,
		expand.BsonLength:    lengthProject,
	})
}
