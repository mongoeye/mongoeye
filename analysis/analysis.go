// Package analysis is used to assemble analysis from individual stages.
package analysis

import (
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2"
	"time"
)

// NameSeparator separates names of nested fields
const NameSeparator = "."

// ArrayItemMark represents array item in full field name
const ArrayItemMark = "[]"

// AggregationMinVersion is minimal MongoDB version that allows analysis using aggregation framework
var AggregationMinVersion = []int{3, 5, 9}

// AggregationMinVersionStr (string) is minimal MongoDB version that allows analysis using aggregation framework
var AggregationMinVersionStr = "3.5.9"

// RandomSampleMinVersion is minimal MongoDB version that allows analysis using random samples
var RandomSampleMinVersion = []int{3, 2, 0}

// RandomSampleMinVersionStr (string) is minimal MongoDB version that allows analysis using random samples
var RandomSampleMinVersionStr = "3.2.0"

// Analysis consists of the options, target collection and the four contiguous stages.
type Analysis struct {
	options     *Options        // common options
	collection  *mgo.Collection // target collection
	sampleStage *Stage          // data sampling
	expandStage *Stage          // extract values from document fields
	groupStage  *Stage          // grouping values with the same field name and type, aggregation calculation
	mergeStage  *Stage          // merge different types of the same field
}

// Options for all stages of analysis.
type Options struct {
	Location    *time.Location // time location for calculations with dates
	Concurrency int            // number of parallel processes for local calculations
	BufferSize  int            // buffer size between phases
	BatchSize   int            // number of documents in one batch from the database
}

// Stage can be represented by a pipeline that runs in the database
// or by a processor function that runs locally.
// If both are used, then the pipeline results
// are passed to the input of the processor function.
type Stage struct {
	PipelineFactory PipelineFactory
	Processor       Processor
}

// PipelineFactory generate pipeline according analysis options.
type PipelineFactory func(analysisOptions *Options) *expr.Pipeline

// Processor function has a channel from the previous stage at its input.
// Return value is output channel that fed to the next stage.
// The input of the first stage is raw (binary) data from the database.
// The output of the last stage are the final results.
type Processor func(inputCh interface{}, options *Options) interface{}

// NewAnalysis - analysis factory.
func NewAnalysis(options *Options) Analysis {
	return Analysis{options: options}
}

// SetSampleStage set sample stage of analysis.
// Task of the sample stage is to select the desired sample of documents
// from the collection and pass them to next stages of analysis.
func (a *Analysis) SetSampleStage(stage *Stage) {
	a.sampleStage = stage
}

// SetExpandStage set expand stage of analysis.
// Expand stage expands documents to value (result: [name, type] => value).
// Individual fields in this stage lose their link to the original document
// that allows analyzing all values of some field in next stages.
func (a *Analysis) SetExpandStage(stage *Stage) {
	a.expandStage = stage
}

// SetGroupStage set group stage of analysis.
// Group stage group values from expand stage.
// The values are grouped under the same name and type (result: [name, type] => value aggregation).
// This stage counts all statistics above the data.
func (a *Analysis) SetGroupStage(stage *Stage) {
	a.groupStage = stage
}

// SetMergeStage set merge stage of analysis.
// Merge stage merge different types of the same field (result: [name] => types aggregation).
func (a *Analysis) SetMergeStage(stage *Stage) {
	a.mergeStage = stage
}

// SetCollection set target collection.
func (a *Analysis) SetCollection(c *mgo.Collection) {
	a.collection = c
}

// Run the analysis on the selected collection.
func (a *Analysis) Run() interface{} {
	stages := []*Stage{
		a.sampleStage,
		a.expandStage,
		a.groupStage,
		a.mergeStage,
	}

	dbPipeline, in, out := LinkStages(stages, a.options)

	dbPipeline.ToRawChannel(
		a.collection,
		in,
		a.options.Concurrency,
		a.options.BufferSize,
		a.options.BatchSize,
	)

	return out
}
