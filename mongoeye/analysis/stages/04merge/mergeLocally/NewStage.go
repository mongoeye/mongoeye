// Package mergeLocally is the implementation of the merge stage that runs locally.
package mergeLocally

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"github.com/mongoeye/mongoeye/analysis/stages/04merge"
	"sort"
	"strings"
)

// NewStage - MergeLocally stage factory
func NewStage(mergeOptions *merge.Options) *analysis.Stage {
	return &analysis.Stage{
		Processor: func(_input interface{}, analysisOptions *analysis.Options) interface{} {
			// Input channel
			input := group.ToResultChannel(_input, analysisOptions.Location, analysisOptions.Concurrency, analysisOptions.BufferSize)

			// Create output channel and wait group
			output := make(chan analysis.Field, analysisOptions.BufferSize)

			// Merge
			go func() {
				m := make(map[string]*analysis.Field)

				for gr := range input {
					// Load or create
					r := m[gr.Name]
					if r == nil {
						r = &analysis.Field{
							Name: gr.Name,
						}
						m[gr.Name] = r
					}

					t := gr.Type

					r.Count += t.Count
					r.Types = append(r.Types, &t)
				}

				for _, f := range m {
					f.Level = uint(strings.Count(f.Name, analysis.NameSeparator))
					sort.Sort(f.Types)

					output <- *f
				}

				close(output)
			}()

			return output
		},
	}
}
