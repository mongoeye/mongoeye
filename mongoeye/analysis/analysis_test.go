package analysis

import (
	"github.com/mongoeye/mongoeye/helpers"
	"github.com/mongoeye/mongoeye/tests"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
	"testing"
	"time"
)

func TestNewAnalysis(t *testing.T) {
	analysis := NewAnalysis(&Options{
		Location:    time.UTC,
		Concurrency: 2,
		BufferSize:  100,
		BatchSize:   50,
	})

	assert.IsType(t, Analysis{}, analysis)
	assert.Equal(t, time.UTC, analysis.options.Location)
	assert.Equal(t, 2, analysis.options.Concurrency)
	assert.Equal(t, 100, analysis.options.BufferSize)
	assert.Equal(t, 50, analysis.options.BatchSize)
}

func TestAnalysis_SetSampleStage(t *testing.T) {
	stage := &Stage{}
	analysis := Analysis{}
	analysis.SetSampleStage(stage)
	assert.Equal(t, stage, analysis.sampleStage)
}

func TestAnalysis_SetExpandStage(t *testing.T) {
	stage := &Stage{}
	analysis := Analysis{}
	analysis.SetExpandStage(stage)
	assert.Equal(t, stage, analysis.expandStage)
}

func TestAnalysis_SetGroupStage(t *testing.T) {
	stage := &Stage{}
	analysis := Analysis{}
	analysis.SetGroupStage(stage)
	assert.Equal(t, stage, analysis.groupStage)
}

func TestAnalysis_SetMergeStage(t *testing.T) {
	stage := &Stage{}
	analysis := Analysis{}
	analysis.SetMergeStage(stage)
	assert.Equal(t, stage, analysis.mergeStage)
}

func TestAnalysis_Run(t *testing.T) {
	c := tests.CreateTestCollection(tests.TestDbSession)
	defer tests.DropTestCollection(c)

	c.Insert(
		bson.M{"test": "abc"},
		bson.M{"test": "def"},
	)

	sampleStage := &Stage{
		Processor: func(inputCh interface{}, options *Options) interface{} {
			outCh := make(chan bson.M)

			if ch, ok := inputCh.(chan []byte); ok {
				go func() {
					for raw := range ch {
						m := bson.M{}
						bson.Unmarshal(raw, m)
						outCh <- m
					}
					close(outCh)
				}()
			} else {
				panic("Unexpected type.")
			}

			return outCh
		},
	}

	expandStage := &Stage{
		Processor: func(inputCh interface{}, options *Options) interface{} {
			outCh := make(chan bson.M)

			if ch, ok := inputCh.(chan bson.M); ok {
				go func() {
					for m := range ch {
						outCh <- bson.M{
							"test": helpers.SafeToString(m["test"]) + "_e",
						}
					}
					close(outCh)
				}()
			} else {
				panic("Unexpected type.")
			}

			return outCh
		},
	}

	groupStage := &Stage{
		Processor: func(inputCh interface{}, options *Options) interface{} {
			outCh := make(chan bson.M)

			if ch, ok := inputCh.(chan bson.M); ok {
				go func() {
					for m := range ch {
						outCh <- bson.M{
							"test": helpers.SafeToString(m["test"]) + "_g",
						}
					}
					close(outCh)
				}()
			} else {
				panic("Unexpected type.")
			}

			return outCh
		},
	}

	mergeStage := &Stage{
		Processor: func(inputCh interface{}, options *Options) interface{} {
			outCh := make(chan bson.M)

			if ch, ok := inputCh.(chan bson.M); ok {
				go func() {
					for m := range ch {
						outCh <- bson.M{
							"test": helpers.SafeToString(m["test"]) + "_m",
						}
					}
					close(outCh)
				}()
			} else {
				panic("Unexpected type.")
			}

			return outCh
		},
	}

	analysis := NewAnalysis(&Options{
		Location:    time.UTC,
		Concurrency: 1,
		BufferSize:  100,
		BatchSize:   50,
	})
	analysis.SetCollection(c)
	analysis.SetSampleStage(sampleStage)
	analysis.SetExpandStage(expandStage)
	analysis.SetGroupStage(groupStage)
	analysis.SetMergeStage(mergeStage)

	outCh := analysis.Run()

	if ch, ok := outCh.(chan bson.M); ok {
		assert.Equal(t, bson.M{"test": "abc_e_g_m"}, <-ch)
		assert.Equal(t, bson.M{"test": "def_e_g_m"}, <-ch)

		_, ok = <-ch
		assert.Equal(t, ok, false)
	} else {
		panic("Unexpected type.")
	}
}
