package expr

import (
	"sync"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// Pipeline represents stages of aggregation pipeline in MongoDB.
type Pipeline struct {
	stages []bson.M
}

// NewPipeline creates new Pipeline.
func NewPipeline() *Pipeline {
	return &Pipeline{}
}

func (p *Pipeline) add(stage bson.M) {
	p.stages = append(p.stages, stage)
}

// AddStage to Pipeline.
func (p *Pipeline) AddStage(op string, spec interface{}) {
	p.add(bson.M{
		("$" + op): spec,
	})
}

// AddStages to Pipeline.
func (p *Pipeline) AddStages(stages ...bson.M) {
	for _, stage := range stages {
		p.add(stage)
	}
}

// GetStages from pipeline.
func (p *Pipeline) GetStages() []bson.M {
	return p.stages
}

// GetPipe gets MongoDB aggregation pipe.
func (p *Pipeline) GetPipe(c *mgo.Collection) *mgo.Pipe {
	pipe := c.Pipe(p.GetStages())
	pipe.AllowDiskUse()
	return pipe
}

// Iter - gets iterator over Pipeline results.
func (p *Pipeline) Iter(c *mgo.Collection, batchSize int) *mgo.Iter {
	if batchSize < 1 {
		panic("Value of 'batchSize' argument must be at least 1.")
	}

	pipe := p.GetPipe(c)
	if batchSize > 0 {
		pipe.Batch(batchSize)
	}
	return pipe.Iter()
}

// ToRawChannel - gets pipeline results as raw ([]byte) channel.
func (p *Pipeline) ToRawChannel(c *mgo.Collection, outCh chan<- []byte, concurrency int, bufferSize int, batchSize int) {
	if concurrency < 1 {
		panic("Value of 'concurrency' argument must be at least 1.")
	}

	if bufferSize < 0 {
		panic("Value of 'bufferSize' argument must be at least 0.")
	}

	iterator := p.Iter(c, batchSize)

	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			raw := new(bson.Raw)

			for {
				if iterator.Next(raw) {
					outCh <- raw.Data
				} else {
					break
				}
			}

			closeIterator(iterator)
		}()
	}

	// Close channel on end
	go func() {
		wg.Wait()
		close(outCh)
	}()
}

// ToBsonChannel - gets pipeline results as BSON channel.
func (p *Pipeline) ToBsonChannel(c *mgo.Collection, outCh chan<- bson.M, concurrency int, bufferSize int, batchSize int) {
	if concurrency < 1 {
		panic("Value of 'concurrency' argument must be at least 1.")
	}

	if bufferSize < 0 {
		panic("Value of 'bufferSize' argument must be at least 0.")
	}

	iterator := p.Iter(c, batchSize)

	wg := sync.WaitGroup{}
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			for {
				m := make(bson.M)
				if iterator.Next(m) {
					outCh <- m
				} else {
					break
				}
			}

			closeIterator(iterator)
		}()
	}

	// Close channel on end
	go func() {
		wg.Wait()
		close(outCh)
	}()
}

// Properly close iterator
func closeIterator(iterator *mgo.Iter) {
	if err := iterator.Close(); err != nil {
		panic(err)
	}
}
