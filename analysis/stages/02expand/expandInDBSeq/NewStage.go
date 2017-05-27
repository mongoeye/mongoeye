// Package expandInDBSeq is the implementation of the expand stage that runs in database.
package expandInDBSeq

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand/expandInDBCommon"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
	"strconv"
)

// NewStage - ExpandInDBSeq stage factory.
func NewStage(expandOptions *expand.Options) *analysis.Stage {
	return &analysis.Stage{
		PipelineFactory: func(analysisOptions *analysis.Options) *expr.Pipeline {
			// Create pipeline
			p := expr.NewPipeline()

			// Root level
			p.AddStage("project", bson.M{
				analysis.BsonId:   0,
				expand.BsonNested: processObject(expr.Var("ROOT"), 0, expandOptions),
			})
			p.AddStage("unwind", expr.Field(expand.BsonNested))

			// Deeper levels
			var level uint
			for ; level <= expandOptions.MaxDepth; level++ {
				isLast := level == expandOptions.MaxDepth
				p.AddStage("replaceRoot", bson.M{
					// Skip fields without nested fields
					"newRoot": expr.Cond(
						expr.Eq(expr.Field(expand.BsonNested), nil),
						expr.Var("ROOT"),
						processNestedFields(isLast, level, expandOptions),
					),
				})

				p.AddStage("unwind", bson.M{
					"path": expr.Field(expand.BsonNested),
					"preserveNullAndEmptyArrays": true,
				})
			}

			// Remove empty property NESTED
			p.AddStage("project", bson.M{expand.BsonNested: 0})

			return p
		},
	}
}

// Extract fields from object.
func processObject(object interface{}, level uint, expandOptions *expand.Options) interface{} {
	// Each level has its own different variable to avoid collisions
	itemVar := "xl" + strconv.Itoa(int(level))
	field := expandInDBCommon.FieldVars{
		Name:  expr.Var(itemVar + ".k"),
		Type:  expr.Var(expand.BsonFieldType),
		Value: expr.Var(itemVar + ".v"),
		Level: level,
	}

	return expr.Map(
		expr.ObjectToArray(object),
		itemVar,
		expr.Let(
			bson.M{expand.BsonFieldType: expr.Type(field.Value)},
			processField(field, expandOptions),
		),
	)
}

// Process object or array field.
func processField(field expandInDBCommon.FieldVars, expandOptions *expand.Options) interface{} {
	// Type switch
	sw := expr.Switch()

	// String
	sw.AddBranch(
		expr.Eq(field.Type, "string"),
		expandInDBCommon.ProcessStringField(field.Name, field, expandOptions),
	)

	// Array
	sw.AddBranch(
		expr.Eq(field.Type, "array"),
		processArrayField(field.Name, field, expandOptions),
	)

	// Object
	sw.AddBranch(
		expr.Eq(field.Type, "object"),
		processObjectField(field.Name, field, expandOptions),
	)

	// Scalar types
	sw.SetDefault(expandInDBCommon.ProcessScalarField(field.Name, field, expandOptions))

	return sw.Bson()
}

// Process array field.
func processObjectField(fullName interface{}, field expandInDBCommon.FieldVars, expandOptions *expand.Options) bson.M {
	m := bson.M{
		expand.BsonFieldName: fullName,
		expand.BsonFieldType: field.Type,
		expand.BsonLevel:     field.Level,
		expand.BsonNested:    field.Value,
	}

	if expandOptions.StoreValue {
		m[expand.BsonValue] = field.Value
	}

	if expandOptions.StoreObjectLength {
		m[expand.BsonLength] = expr.Size(expr.ObjectToArray(field.Value))
	}

	return m
}

// Process array field.
func processArrayField(fullName interface{}, field expandInDBCommon.FieldVars, expandOptions *expand.Options) bson.M {
	m := bson.M{
		expand.BsonFieldName: fullName,
		expand.BsonFieldType: field.Type,
		expand.BsonLevel:     field.Level,
		expand.BsonNested:    expr.Var("xslice"),
	}

	// Array length
	if expandOptions.StoreArrayLength {
		m[expand.BsonLength] = expr.Size(field.Value)
	}

	if expandOptions.StoreValue {
		m[expand.BsonValue] = expr.Var("xslice")
	}

	return expr.Let(
		bson.M{"xslice": expr.Slice(field.Value, expandOptions.ArrayMaxLength)},
		m,
	)
}

// Process nested fields one level below.
func processNestedFields(isLast bool, level uint, expandOptions *expand.Options) bson.M {
	analysisNested := !isLast // analysis nested documents to a specified depth
	prefix := expr.Field(expand.BsonNested) + analysis.NameSeparator

	// Field name
	var fullName interface{}
	if level == 0 {
		// On first level preserve original name eg. "car"
		fullName = prefix + expand.BsonFieldName
	} else {
		prefix = expr.Field(expand.BsonNested) + analysis.NameSeparator
		// On deeper level prepend name with parent eg. "car.color"
		fullName = expr.Concat(
			expr.Field(expand.BsonFieldName),
			analysis.NameSeparator,
			prefix+expand.BsonFieldName,
		)
	}

	sw := expr.Switch()

	// String
	sw.AddBranch(
		expr.Eq(prefix+expand.BsonFieldType, "string"),
		processNestedStringField(fullName, prefix, level, expandOptions),
	)

	// Object
	sw.AddBranch(
		expr.Eq(prefix+expand.BsonFieldType, "object"),
		processNestedObjectField(fullName, prefix, analysisNested, level, expandOptions),
	)

	// Array
	sw.AddBranch(
		expr.Eq(prefix+expand.BsonFieldType, "array"),
		processNestedArrayField(fullName, prefix, analysisNested, level, expandOptions),
	)

	// Scalar types
	sw.SetDefault(processNestedScalarField(fullName, prefix, level, expandOptions))

	return sw.Bson()
}

func processNestedStringField(fullName interface{}, prefix string, level uint, expandOptions *expand.Options) bson.M {
	m := bson.M{
		expand.BsonFieldName: fullName,
		expand.BsonFieldType: prefix + expand.BsonFieldType,
		expand.BsonLevel:     level,
		expand.BsonNested:    nil,
	}

	if expandOptions.StoreStringLength {
		m[expand.BsonLength] = prefix + expand.BsonLength
	}

	if expandOptions.StoreValue {
		m[expand.BsonValue] = prefix + expand.BsonValue
	}

	return m
}

func processNestedObjectField(fullName interface{}, prefix string, analysisNested bool, level uint, expandOptions *expand.Options) bson.M {
	m := bson.M{
		expand.BsonFieldName: fullName,
		expand.BsonFieldType: prefix + expand.BsonFieldType,
		expand.BsonLevel:     level,
	}

	if expandOptions.StoreValue {
		m[expand.BsonValue] = prefix + expand.BsonValue
	}

	if expandOptions.StoreObjectLength {
		m[expand.BsonLength] = prefix + expand.BsonLength
	}

	if analysisNested {
		m[expand.BsonNested] = expr.ConcatArrays(
			[]interface{}{nil}, // null represent parent field
			processObject(prefix+expand.BsonNested, level+1, expandOptions),
		)
	} else {
		m[expand.BsonNested] = nil
	}

	return m
}

func processNestedArrayField(fullName interface{}, prefix string, analysisNested bool, level uint, expandOptions *expand.Options) bson.M {
	m := bson.M{
		expand.BsonFieldName: fullName,
		expand.BsonFieldType: prefix + expand.BsonFieldType,
		expand.BsonLevel:     level,
	}

	// Array length
	if expandOptions.StoreArrayLength {
		m[expand.BsonLength] = prefix + expand.BsonLength
	}

	if expandOptions.StoreValue {
		m[expand.BsonValue] = prefix + expand.BsonValue
	}

	if analysisNested {
		field := expandInDBCommon.FieldVars{
			Name:  analysis.ArrayItemMark,
			Type:  expr.Var(expand.BsonFieldType),
			Value: expr.Var("i"),
			Level: level + 1,
		}

		m[expand.BsonNested] = expr.ConcatArrays(
			[]interface{}{nil}, // null represent parent field
			expr.Map(
				prefix+expand.BsonNested,
				"i",
				expr.Let(
					bson.M{expand.BsonFieldType: bson.M{"$type": field.Value}},
					processField(field, expandOptions),
				),
			),
		)
	} else {
		m[expand.BsonNested] = nil
	}

	return m
}

func processNestedScalarField(fullName interface{}, prefix string, level uint, expandOptions *expand.Options) bson.M {
	m := bson.M{
		expand.BsonFieldName: fullName,
		expand.BsonFieldType: prefix + expand.BsonFieldType,
		expand.BsonLevel:     level,
		expand.BsonNested:    nil,
	}

	if expandOptions.StoreValue {
		m[expand.BsonValue] = prefix + expand.BsonValue
	}

	return m
}
