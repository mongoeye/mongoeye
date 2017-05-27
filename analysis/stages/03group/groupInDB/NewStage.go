// Package groupInDB is the implementation of the group stage that runs in database.
package groupInDB

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/mongo/expr"
)

// NewStage - GroupInDB stage factory
func NewStage(groupOptions *group.Options) *analysis.Stage {
	return &analysis.Stage{
		PipelineFactory: func(analysisOptions *analysis.Options) *expr.Pipeline {
			p := expr.NewPipeline()
			GroupValues(p, groupOptions)
			ComputeStats(p, groupOptions, analysisOptions)
			GroupStats(p, groupOptions)
			return p
		},
	}
}
