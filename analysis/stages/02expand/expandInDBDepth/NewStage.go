// Package expandInDBDepth is the implementation of the expand stage that runs in database.
package expandInDBDepth

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand/expandInDBCommon"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
	"strconv"
)

// NewStage - ExpandInDBDepth stage factory.
func NewStage(expandOptions *expand.Options) *analysis.Stage {
	return &analysis.Stage{
		PipelineFactory: func(analysisOptions *analysis.Options) *expr.Pipeline {
			// Create pipeline
			p := expr.NewPipeline()

			// Extract fields from nested documents
			p.AddStage("project", bson.M{
				analysis.BsonId:   0,
				expand.BsonNested: processObject([]interface{}{}, expr.Var("ROOT"), 0, expandOptions),
			})

			// Expand nested fields to specified depth
			for level := uint(0); level <= expandOptions.MaxDepth; level++ {
				p.AddStage("unwind", bson.M{
					"path":                       expr.Field(expand.BsonNested),
					"preserveNullAndEmptyArrays": true,
				})
				p.AddStage("replaceRoot", bson.M{
					// Skip fields without nested fields
					"newRoot": expr.Cond(expr.Eq(expr.Field(expand.BsonNested), nil),
						expr.Var("ROOT"),
						expr.Field(expand.BsonNested),
					),
				})
			}

			// Remove empty property NESTED
			p.AddStage("project", bson.M{expand.BsonNested: 0})

			return p
		},
	}
}

// Extract fields from object ("recursive" through processField to max depth).
func processObject(superiors []interface{}, object interface{}, level uint, expandOptions *expand.Options) interface{} {
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
			processField(superiors[:], field, expandOptions),
		),
	)
}

// Process elements from array.
func processArray(superiors []interface{}, field expandInDBCommon.FieldVars, expandOptions *expand.Options) bson.M {
	itemVar := "xa" + strconv.Itoa(int(field.Level))

	item := expandInDBCommon.FieldVars{
		Name:  analysis.ArrayItemMark,
		Type:  expr.Var(expand.BsonFieldType),
		Value: expr.Var(itemVar),
		Level: field.Level + 1,
	}

	return expr.Map(
		field.Value,
		itemVar,
		expr.Let(
			bson.M{expand.BsonFieldType: expr.Type(item.Value)},
			processField(
				append(superiors, field.Name),
				item,
				expandOptions,
			),
		),
	)
}

// Process field of object or array.
func processField(superiors []interface{}, field expandInDBCommon.FieldVars, expandOptions *expand.Options) interface{} {
	// Generate full name from superiors names separated by NAME_SEPARATOR
	var fullName interface{}
	if len(superiors) == 0 {
		fullName = field.Name
	} else {
		var parts []interface{}
		// Separate superiors names with dot
		for _, s := range superiors {
			parts = append(parts, s)
			parts = append(parts, analysis.NameSeparator)
		}
		parts = append(parts, field.Name)

		fullName = expr.Concat(parts...)
	}

	// Type switch
	sw := expr.Switch()

	// String
	sw.AddBranch(
		expr.Eq(field.Type, "string"),
		expandInDBCommon.ProcessStringField(fullName, field, expandOptions),
	)

	// Array
	sw.AddBranch(
		expr.Eq(field.Type, "array"),
		processArrayField(superiors, fullName, field, expandOptions),
	)

	// Object
	sw.AddBranch(
		expr.Eq(field.Type, "object"),
		processObjectField(superiors, fullName, field, expandOptions),
	)

	// Scalar types
	sw.SetDefault(expandInDBCommon.ProcessScalarField(fullName, field, expandOptions))

	return sw.Bson()
}

// Process array field.
func processArrayField(superiors []interface{}, fullName interface{}, field expandInDBCommon.FieldVars, expandOptions *expand.Options) bson.M {
	m := bson.M{
		expand.BsonFieldName: fullName,
		expand.BsonFieldType: field.Type,
		expand.BsonLevel:     field.Level,
	}

	// Array length
	if expandOptions.StoreArrayLength {
		m[expand.BsonLength] = expr.Size(field.Value)
	}

	// Limit array size
	field.Value = expr.Slice(field.Value, expandOptions.ArrayMaxLength)

	// Array items
	if field.Level >= expandOptions.MaxDepth {
		m[expand.BsonNested] = nil
	} else {
		m[expand.BsonNested] = expr.ConcatArrays(
			[]interface{}{nil}, // null represent parent field
			processArray(superiors, field, expandOptions),
		)
	}

	if expandOptions.StoreValue {
		m[expand.BsonValue] = field.Value
	}

	return m
}

// Process array field.
func processObjectField(superiors []interface{}, fullName interface{}, field expandInDBCommon.FieldVars, expandOptions *expand.Options) bson.M {
	m := bson.M{
		expand.BsonFieldName: fullName,
		expand.BsonFieldType: field.Type,
		expand.BsonLevel:     field.Level,
	}

	if expandOptions.StoreValue {
		m[expand.BsonValue] = field.Value
	}

	// Nested fields
	if field.Level >= expandOptions.MaxDepth {
		m[expand.BsonNested] = nil
		if expandOptions.StoreObjectLength {
			m[expand.BsonLength] = expr.Size(expr.ObjectToArray(field.Value))
		}
	} else {
		fields := processObject(
			append(superiors, field.Name),
			field.Value,
			field.Level+1,
			expandOptions,
		)

		if expandOptions.StoreObjectLength {
			fieldsVar := "xf"

			m[expand.BsonNested] = expr.ConcatArrays(
				[]interface{}{nil}, // null represent parent field
				expr.Var(fieldsVar),
			)

			m[expand.BsonLength] = expr.Size(expr.Var(fieldsVar))

			m = expr.Let(
				bson.M{fieldsVar: fields},
				m,
			)
		} else {
			m[expand.BsonNested] = expr.ConcatArrays(
				[]interface{}{nil}, // null represent parent field
				fields,
			)
		}
	}

	return m
}
