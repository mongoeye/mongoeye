package analysis

import (
	"fmt"
	"github.com/mongoeye/mongoeye/mongo/expr"
)

var stageNames = [...]string{
	"sample",
	"expand",
	"group",
	"merge",
}

// LinkStages links individual stages together.
func LinkStages(stages []*Stage, options *Options) (*expr.Pipeline, chan<- []byte, interface{}) {
	// Create pipeline
	p := expr.NewPipeline()

	// Channels
	var in = make(chan []byte, options.BufferSize)
	var channel interface{} = in

	// Defines if actual stage is performed in databases or locally.
	inDatabase := true

	// Validate and link stages together
	for i, s := range stages {
		name := stageNames[i]
		assertStageNotEmpty(name, s)

		stage := *s

		// Pipeline (computation) thar runs in database.
		if stage.PipelineFactory != nil {
			if inDatabase {
				// Append pipeline to global pipeline
				p.AddStages(stage.PipelineFactory(options).GetStages()...)
			} else {
				// If is stage defined with Pipeline then all previous stages must be too.
				panic(fmt.Sprintf("Stage %s is defined by PipelineFactory, but the processing of data in the database has been completed in some previous stage.", name))
			}
		}

		// Pipeline (computation) that runs locally.
		if stage.Processor != nil {
			inDatabase = false

			// Include processor to pipeline
			channel = stage.Processor(channel, options)
		}
	}

	return p, in, channel
}

func assertStageNotEmpty(name string, stage *Stage) {
	if (*stage).PipelineFactory == nil && (*stage).Processor == nil {
		panic(fmt.Sprintf("Incorrect stage %s. analysis stage cannot has empty both PipelineFactory and Processor.", name))
	}
}
