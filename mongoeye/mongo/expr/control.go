package expr

import (
	"gopkg.in/mgo.v2/bson"
	"sort"
	"strings"
)

// Field - document field shorthand
func Field(name ...string) string {
	return "$" + strings.Join(name, ".")
}

// Var - local variable
func Var(name ...string) string {
	return "$$" + strings.Join(name, ".")
}

// Eq encapsulates MongoDB operation $eq.
func Eq(a interface{}, b interface{}) bson.M {
	return bson.M{"$eq": []interface{}{a, b}}
}

// Ne encapsulates MongoDB operation $ne.
func Ne(a interface{}, b interface{}) bson.M {
	return bson.M{"$ne": []interface{}{a, b}}
}

// Sw encapsulates MongoDB operation $switch.
type sw struct {
	branches []bson.M
	def      interface{}
}

// Switch encapsulates MongoDB operation $switch.
func Switch() *sw {
	return &sw{}
}

// SetDefault branch of switch.
func (s *sw) SetDefault(def interface{}) {
	s.def = def
}

// AddBranch to switch.
func (s *sw) AddBranch(_case interface{}, _then interface{}) {
	s.branches = append(s.branches, bson.M{
		"case": _case,
		"then": _then,
	})
}

// Bson representation of switch statement.
func (s *sw) Bson() bson.M {
	return bson.M{
		"$switch": bson.M{
			"branches": s.branches,
			"default":  s.def,
		},
	}
}

// Cond encapsulates MongoDB operation $cond.
func Cond(cond interface{}, t interface{}, f interface{}) bson.M {
	return bson.M{"$cond": []interface{}{cond, t, f}}
}

// Let encapsulates MongoDB operation $let.
func Let(vars bson.M, in interface{}) bson.M {
	return bson.M{
		"$let": bson.M{
			"vars": vars,
			"in":   in,
		},
	}
}

// Type encapsulates MongoDB operation $type.
func Type(el interface{}) bson.M {
	return bson.M{"$type": el}
}

// Facet encapsulates MongoDB operation $facet.
type facet struct {
	fields map[string](*Pipeline)
}

// Facet encapsulates MongoDB operation $facet.
func Facet() *facet {
	return &facet{
		fields: make(map[string](*Pipeline)),
	}
}

// AddField to facet.
func (f *facet) AddField(name string, p *Pipeline) *Pipeline {
	if p == nil {
		p = new(Pipeline)
	}
	f.fields[name] = p
	return p
}

// GetField - sub-pipeline from facet.
func (f *facet) GetField(name string) *Pipeline {
	return f.fields[name]
}

// GetMap fields => stages..
func (f *facet) GetMap() bson.M {
	m := bson.M{}

	for n, p := range f.fields {
		m[n] = p.GetStages()
	}

	return m
}

// GetKeys - get facet keys.
func (f *facet) GetKeys() []string {
	keys := []string{}

	for k := range f.fields {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// GetKeysAsFields - get facet keys as fields.
func (f *facet) GetKeysAsFields() []interface{} {
	fields := []interface{}{}

	for k := range f.fields {
		fields = append(fields, Field(k))
	}

	return fields
}

// MergeObjects encapsulates MongoDB operation $mergeObjects.
func MergeObjects(objects interface{}) bson.M {
	return bson.M{
		"$mergeObjects": objects,
	}
}
