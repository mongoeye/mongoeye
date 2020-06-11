// Package merge represents merge stage of analysis.
// Merge stage merge different types of the same field (result: [name] => types aggregation).
package merge

import "github.com/mongoeye/mongoeye/analysis"

// Options influence the results of merge stage
type Options struct {
}

// StageFactory prototype.
type StageFactory func(options *Options) *analysis.Stage
