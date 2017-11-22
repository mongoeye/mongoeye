// Package sample represents sample stage of analysis.
// Task of the sample stage is to select the desired sample of documents
// from the collection and pass them to next stages of analysis.
package sample

import (
	"github.com/mongoeye/mongoeye/analysis"
	"gopkg.in/mgo.v2/bson"
)

// Options  of sample stage.
type Options struct {
	Match   bson.M
	Project bson.M
	Method  SampleMethod
	Limit   uint64
}

// SampleMethod defines the method of sampling (all, first, last, random documents).
type SampleMethod uint8

const (
	// AllDocuments - analyse all documents in collection
	AllDocuments SampleMethod = iota

	// FirstNDocuments - analyse first N documents in collection
	FirstNDocuments

	// LastNDocuments - analyse last N documents in collection
	LastNDocuments

	// RandomNDocuments - analyse random N documents in collection
	RandomNDocuments
)

// StageFactory prototype.
type StageFactory func(sampleOptions *Options) *analysis.Stage
