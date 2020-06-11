package merge

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/03group"
	"gopkg.in/mgo.v2/bson"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

// ToFieldChannel converts input channel to Field channel.
// The input may by raw []byte channel or Field channel (no conversion).
func ToFieldChannel(inCh interface{}, location *time.Location, concurrency int, bufferSize int) <-chan analysis.Field {
	if ch, ok := inCh.(chan analysis.Field); ok {
		// local results
		return ch
	} else if inCh, ok := inCh.(chan []byte); ok {
		// database results, bson -> chan Field
		outCh := make(chan analysis.Field, bufferSize)

		wg := &sync.WaitGroup{}
		wg.Add(concurrency)

		// Run workers
		for i := 0; i < concurrency; i++ {
			go convertWorker(inCh, outCh, location, wg)
		}

		// Wait for workers
		go func() {
			wg.Wait()
			close(outCh)
		}()

		return outCh
	}

	panic("Invalid input. Expected 'chan expand.Value' or 'chan []byte'. Given: " + reflect.TypeOf(inCh).String())
}

func convertWorker(inCh <-chan []byte, outCh chan<- analysis.Field, location *time.Location, wg *sync.WaitGroup) {
	defer wg.Done()

	for bin := range inCh {
		f := analysis.Field{}
		bson.Unmarshal(bin, &f)

		f.Level = uint(strings.Count(f.Name, analysis.NameSeparator))

		for _, t := range f.Types {
			group.NormalizeType(t, location)
		}

		sort.Sort(f.Types)

		outCh <- f
	}
}

// FieldChannelToSlice reads Field channel to slice.
func FieldChannelToSlice(ch <-chan analysis.Field) analysis.Fields {
	out := make(analysis.Fields, 0)

	for {
		v, ok := <-ch
		if !ok {
			break
		}
		out = append(out, &v)
	}

	sort.Sort(out)

	return out
}
