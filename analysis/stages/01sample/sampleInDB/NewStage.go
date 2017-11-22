// Package sampleInDB is the implementation of the sampling stage.
package sampleInDB

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/01sample"
	"github.com/mongoeye/mongoeye/mongo/expr"
	"gopkg.in/mgo.v2/bson"
)

// NewStage - SampleInDB stage factory.
func NewStage(sampleOptions *sample.Options) *analysis.Stage {
	return &analysis.Stage{
		PipelineFactory: func(analysisOptions *analysis.Options) *expr.Pipeline {
			// Create pipeline
			p := expr.NewPipeline()

			// Match
			if len(sampleOptions.Match) != 0 {
				p.AddStage("match", sampleOptions.Match)
			}

			// Scope
			switch sampleOptions.Scope {
			case sample.First:
				p.AddStage("sort", bson.M{"_id": 1})
				p.AddStage("limit", sampleOptions.Limit)
			case sample.Last:
				p.AddStage("sort", bson.M{"_id": -1})
				p.AddStage("limit", sampleOptions.Limit)
			case sample.Random:
				p.AddStage("sample", bson.M{"size": sampleOptions.Limit})
			case sample.All:
				if sampleOptions.Limit != 0 {
					panic("Limit option can not be used together with scope = All. Set limit to 0 or use one of the following scopes: First, Last, Random.")
				}
			default:
				panic("Invalid scope. Use one of the following: First, Last, Random, All.")
			}

			// Project
			if len(sampleOptions.Project) != 0 {
				p.AddStage("project", sampleOptions.Project)
			}

			return p
		},
	}
}
