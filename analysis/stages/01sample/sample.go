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
	Scope   AnalysisScope
	Limit   uint64
}

// AnalysisScope defines the method of sampling.
type AnalysisScope uint8

const (
	// All - analyse all documents in collection
	All AnalysisScope = iota

	// First - analyse first N documents in collection
	First

	// Last - analyse last N documents in collection
	Last

	// Random - analyse random N documents in collection
	Random
)

// StageFactory prototype.
type StageFactory func(sampleOptions *Options) *analysis.Stage
