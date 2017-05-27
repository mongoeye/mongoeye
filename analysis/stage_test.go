package analysis

import (
	"github.com/mongoeye/mongoeye/mongo/expr"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLinkStages_PipelineStageAfterProcessorStage(t *testing.T) {
	stages := []*Stage{
		{
			PipelineFactory: func(analysisOptions *Options) *expr.Pipeline {
				return expr.NewPipeline()
			},
		},
		{
			Processor: func(inputCh interface{}, options *Options) interface{} {
				return make(chan []byte)
			},
		},
		{
			PipelineFactory: func(analysisOptions *Options) *expr.Pipeline {
				return expr.NewPipeline()
			},
		},
		{
			Processor: func(inputCh interface{}, options *Options) interface{} {
				return make(chan []byte)
			},
		},
	}

	assert.Panics(t, func() {
		LinkStages(stages, &Options{
			Location:    time.UTC,
			Concurrency: 4,
			BufferSize:  50,
			BatchSize:   100,
		})
	})
}

func TestLinkStages_EmptyStage(t *testing.T) {
	stages := []*Stage{
		{
			PipelineFactory: func(analysisOptions *Options) *expr.Pipeline {
				return expr.NewPipeline()
			},
		},
		{
		// empty
		},
		{
			PipelineFactory: func(analysisOptions *Options) *expr.Pipeline {
				return expr.NewPipeline()
			},
		},
		{
			Processor: func(inputCh interface{}, options *Options) interface{} {
				return make(chan []byte)
			},
		},
	}

	assert.Panics(t, func() {
		LinkStages(stages, &Options{
			Location:    time.UTC,
			Concurrency: 4,
			BufferSize:  50,
			BatchSize:   100,
		})
	})
}
