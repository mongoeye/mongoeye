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

			// Sample
			switch sampleOptions.Method {
			case sample.FirstNDocuments:
				p.AddStage("sort", bson.M{"_id": 1})
				p.AddStage("limit", sampleOptions.Limit)
			case sample.LastNDocuments:
				p.AddStage("sort", bson.M{"_id": -1})
				p.AddStage("limit", sampleOptions.Limit)
			case sample.RandomNDocuments:
				p.AddStage("sample", bson.M{"size": sampleOptions.Limit})
			case sample.AllDocuments:
				if sampleOptions.Limit != 0 {
					panic("Limit option can not be used together with sample = all. Set limit to 0 or use one of the following samples: first, last, random.")
				}
			default:
				panic("Invalid sample. Use one of the following: first, last, random, all.")
			}

			// Project
			if len(sampleOptions.Project) != 0 {
				p.AddStage("project", sampleOptions.Project)
			}

			return p
		},
	}
}
