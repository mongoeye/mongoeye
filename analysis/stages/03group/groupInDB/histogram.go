package groupInDB

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
)

func generateHistogram(p *expr.Pipeline, histogramType string, minField string, maxField string, valueField string, valueType string, resolution uint) {
	nameField := analysis.BsonId + "." + group.BsonFieldName
	typeField := analysis.BsonId + "." + analysis.BsonFieldType

	// Convert min/max date values to timestamp
	p.AddStage("project", bson.M{
		analysis.BsonId: 1,
		minField: expr.Cond(
			expr.Eq(valueType, "date"),
			expr.DateToTimestamp(expr.Field(minField)),
			expr.Field(minField),
		),
		maxField: expr.Cond(
			expr.Eq(valueType, "date"),
			expr.DateToTimestamp(expr.Field(maxField)),
			expr.Field(maxField),
		),
		bsonAllValues: 1,
	})

	// range = max - min
	p.AddStage("addFields", bson.M{
		analysis.BsonHistogramRange: expr.Subtract("$"+maxField, "$"+minField),
	})

	// Skip if range == 0
	p.AddStage("match", bson.M{
		analysis.BsonHistogramRange: bson.M{"$ne": 0},
	})

	// Calculate histogram constants (step, range, ...)
	calcHistogramConstants(p, minField, maxField, valueType, resolution)

	// Map values to interval
	p.AddStage("project", bson.M{
		analysis.BsonId:                  1,
		analysis.BsonHistogramStart:      expr.Field(histogramConstants, analysis.BsonHistogramStart),
		analysis.BsonHistogramEnd:        expr.Field(histogramConstants, analysis.BsonHistogramEnd),
		analysis.BsonHistogramRange:      expr.Field(histogramConstants, analysis.BsonHistogramRange),
		analysis.BsonHistogramStep:       expr.Field(histogramConstants, analysis.BsonHistogramStep),
		analysis.BsonHistogramNumOfSteps: expr.Field(histogramConstants, analysis.BsonHistogramNumOfSteps),
		bsonAllValues:                    mapValueToInterval(valueField, valueType),
	})

	p.AddStage("unwind", expr.Field(bsonAllValues))

	p.AddStage("group", bson.M{
		analysis.BsonId: bson.M{
			group.BsonFieldName:        expr.Field(nameField),
			analysis.BsonFieldType:     expr.Field(typeField),
			analysis.BsonIntervalValue: expr.Field(bsonAllValues),
		},
		analysis.BsonHistogramStart:      bson.M{"$first": expr.Field(analysis.BsonHistogramStart)},
		analysis.BsonHistogramEnd:        bson.M{"$first": expr.Field(analysis.BsonHistogramEnd)},
		analysis.BsonHistogramRange:      bson.M{"$first": expr.Field(analysis.BsonHistogramRange)},
		analysis.BsonHistogramStep:       bson.M{"$first": expr.Field(analysis.BsonHistogramStep)},
		analysis.BsonHistogramNumOfSteps: bson.M{"$first": expr.Field(analysis.BsonHistogramNumOfSteps)},
		analysis.BsonIntervalCount:       bson.M{"$sum": 1},
	})

	p.AddStage("sort", bson.M{
		analysis.BsonId + "." + analysis.BsonIntervalValue: 1,
	})

	p.AddStage("group", bson.M{
		analysis.BsonId: bson.M{
			group.BsonFieldName:    expr.Field(nameField),
			analysis.BsonFieldType: expr.Field(typeField),
			statType:               histogramType,
		},
		analysis.BsonHistogramStart:      bson.M{"$first": expr.Field(analysis.BsonHistogramStart)},
		analysis.BsonHistogramEnd:        bson.M{"$first": expr.Field(analysis.BsonHistogramEnd)},
		analysis.BsonHistogramRange:      bson.M{"$first": expr.Field(analysis.BsonHistogramRange)},
		analysis.BsonHistogramStep:       bson.M{"$first": expr.Field(analysis.BsonHistogramStep)},
		analysis.BsonHistogramNumOfSteps: bson.M{"$first": expr.Field(analysis.BsonHistogramNumOfSteps)},
		analysis.BsonHistogramIntervals: bson.M{
			"$push": bson.M{
				analysis.BsonIntervalValue: expr.Field(analysis.BsonId, analysis.BsonIntervalValue),
				analysis.BsonIntervalCount: expr.Field(analysis.BsonIntervalCount),
			},
		},
	})
}

// Calculate histogram constants (step, range, ...) and store them to CONSTANTS field.
func calcHistogramConstants(p *expr.Pipeline, minField string, maxField string, valueType string, resolution uint) {
	// Corresponding GO code:
	// density := (_resolution - 1) / _range
	// shift := math.Pow10(int(math.Floor(math.Log10(density))))
	// normDensity := density / shift
	// divisor := calcDivisorFromNormDensity(normDensity)
	// histogram := group.Histogram{}
	// histogram.Step = 1 / (divisor * shift)
	//if t == "int" ||  t == "int" ||  t == "long" {
	//	// Decimal steps do not make sense for numbers
	//	histogram.Step = math.Ceil(histogram.Step)
	//} else if t == "date" {
	//	// 1,2,5,10,15,30,60 seconds/minutes ... , 1-24 hours ..., x days
	//	histogram.Step = ceilDateStep(histogram.Step)
	//}
	//start := helpers.FloorWithStep(min, histogram.Step)
	//end   := helpers.CeilWithStep(max, histogram.Step) + histogram.Step
	//
	//histogram.Start = fromDoubleTo(t, start, groupOptions)
	//histogram.End   = fromDoubleTo(t, end, groupOptions)
	//histogram.Range = end - start
	//histogram.NumberOfSteps = uint(histogram.Range / histogram.Step)
	p.AddStage("addFields", bson.M{
		histogramConstants: expr.Let(
			bson.M{histogramDensity: expr.Divide(resolution-1, expr.Field(analysis.BsonHistogramRange))},
			expr.Let(
				bson.M{histogramShift: expr.Pow10(expr.Floor(expr.Log10(expr.Var(histogramDensity))))},
				expr.Let(
					bson.M{histogramNormDensity: expr.Divide(expr.Var(histogramDensity), expr.Var(histogramShift))},
					expr.Let(
						bson.M{histogramDivisor: calcDivisorFromNormDensity(expr.Var(histogramNormDensity))},
						expr.Let(
							bson.M{analysis.BsonHistogramStep: expr.Divide(1, expr.Multiply(expr.Var(histogramDivisor), expr.Var(histogramShift)))},
							expr.Let(
								bson.M{analysis.BsonHistogramStep: ceilStep(expr.Var(analysis.BsonHistogramStep), valueType)},
								expr.Let(
									bson.M{
										analysis.BsonHistogramStart: expr.FloorWithStep(expr.Field(minField), expr.Var(analysis.BsonHistogramStep)),
										analysis.BsonHistogramEnd:   expr.Add(expr.CeilWithStep(expr.Field(maxField), expr.Var(analysis.BsonHistogramStep)), expr.Var(analysis.BsonHistogramStep)),
									},
									expr.Let(
										bson.M{analysis.BsonHistogramRange: expr.Subtract(expr.Var(analysis.BsonHistogramEnd), expr.Var(analysis.BsonHistogramStart))},
										bson.M{
											analysis.BsonHistogramStart:      expr.Var(analysis.BsonHistogramStart),
											analysis.BsonHistogramEnd:        expr.Var(analysis.BsonHistogramEnd),
											analysis.BsonHistogramRange:      expr.Var(analysis.BsonHistogramRange),
											analysis.BsonHistogramStep:       expr.Var(analysis.BsonHistogramStep),
											analysis.BsonHistogramNumOfSteps: expr.Divide(expr.Var(analysis.BsonHistogramRange), expr.Var(analysis.BsonHistogramStep)),
										},
									),
								),
							),
						),
					),
				),
			),
		),
	})
}

// Map value to interval according histogram CONSTANTS
func mapValueToInterval(valueField string, valueType string) bson.M {
	if valueType == "int" {
		return expr.Map(
			expr.Field(bsonAllValues),
			"i",
			expr.Floor(
				expr.Divide(
					expr.Subtract(expr.Var("i", valueField), expr.Field(histogramConstants, analysis.BsonHistogramStart)),
					expr.Field(histogramConstants, analysis.BsonHistogramStep),
				),
			),
		)
	}

	return expr.Cond(
		expr.Eq(valueType, "date"),
		// Date
		expr.Map(
			expr.Field(bsonAllValues),
			"i",
			expr.Floor(
				expr.Divide(
					expr.Subtract(expr.DateToTimestamp(expr.Var("i", valueField)), expr.Field(histogramConstants, analysis.BsonHistogramStart)),
					expr.Field(histogramConstants, analysis.BsonHistogramStep),
				),
			),
		),
		// Other fields
		expr.Map(
			expr.Field(bsonAllValues),
			"i",
			expr.Floor(
				expr.Divide(
					expr.Subtract(expr.Var("i", valueField), expr.Field(histogramConstants, analysis.BsonHistogramStart)),
					expr.Field(histogramConstants, analysis.BsonHistogramStep),
				),
			),
		),
	)
}

func ceilStep(step interface{}, t interface{}) bson.M {
	//if t == "int" ||  t == "int" ||  t == "long" {
	//	// Decimal steps do not make sense for numbers
	//	histogram.Step = math.Ceil(histogram.Step)
	//} else if t == "date" {
	//	// 1,2,5,10,15,30,60 seconds/minutes ... , 1-24 hours ..., x days
	//	histogram.Step = ceilDateStep(histogram.Step)
	//}
	if t == "int" || t == "long" {
		return expr.Ceil(step)
	}

	sw := expr.Switch()

	// int, long
	sw.AddBranch(
		expr.Or(
			expr.Eq(t, "int"),
			expr.Eq(t, "long"),
		),
		expr.Ceil(step),
	)

	// date
	sw.AddBranch(
		expr.Eq(t, "date"),
		expr.CeilDateSeconds(step),
	)

	// others
	sw.SetDefault(step)

	return sw.Bson()
}

func calcDivisorFromNormDensity(normDensity interface{}) bson.M {
	sw := expr.Switch()

	sw.AddBranch(
		expr.Lt(normDensity, 2),
		1,
	)

	sw.AddBranch(
		expr.Lt(normDensity, 4),
		2,
	)

	sw.SetDefault(4)

	return sw.Bson()
}
