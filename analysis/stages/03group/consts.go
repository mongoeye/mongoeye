package group

import "github.com/mongoeye/mongoeye/helpers"

// BsonFieldName represents field name
var BsonFieldName string

func init() {
	r := Result{}
	BsonFieldName = helpers.GetBSONFieldName(r, "Name")
}
