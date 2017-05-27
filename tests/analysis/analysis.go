// Package analysisTests contains auxiliary functions for analysis stages testing.
package analysisTests

import (
	"github.com/mongoeye/mongoeye/analysis"
	"gopkg.in/mgo.v2"
	"runtime"
	"time"
)

// RunStages runs analysis for testing purposes.
// It also allows only some stages to run.
func RunStages(c *mgo.Collection, location *time.Location, stages []*analysis.Stage) interface{} {
	options := analysis.Options{
		Location:    location,
		Concurrency: runtime.NumCPU(),
		BufferSize:  5000,
		BatchSize:   500,
	}

	runtime.GOMAXPROCS(options.Concurrency)

	pipeline, in, out := analysis.LinkStages(stages, &options)

	pipeline.ToRawChannel(
		c,
		in,
		2,
		options.BufferSize,
		options.BatchSize,
	)

	return out
}
