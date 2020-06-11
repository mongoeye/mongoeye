package expand

import "github.com/mongoeye/mongoeye/helpers"

// Abbreviations for aggregation pipeline.
var (
	BsonNested    = "_"
	BsonFieldName string
	BsonFieldType string
	BsonLevel     string
	BsonLength    string
	BsonValue     string
)

func init() {
	t := Value{}

	BsonFieldName = helpers.GetBSONFieldName(t, "Name")
	BsonFieldType = helpers.GetBSONFieldName(t, "Type")
	BsonLevel = helpers.GetBSONFieldName(t, "Level")
	BsonLength = helpers.GetBSONFieldName(t, "Length")
	BsonValue = helpers.GetBSONFieldName(t, "Value")
}
