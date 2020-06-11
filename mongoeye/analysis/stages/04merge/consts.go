package merge

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/helpers"
)

// Abbreviations for aggregation pipeline.f
var (
	BsonFieldName string
	BsonCount     string
	BsonTypes     string
)

func init() {
	r := analysis.Field{}

	BsonFieldName = helpers.GetBSONFieldName(r, "Name")
	BsonCount = helpers.GetBSONFieldName(r, "Count")
	BsonTypes = helpers.GetBSONFieldName(r, "Types")
}
