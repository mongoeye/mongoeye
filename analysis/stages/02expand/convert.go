package expand

import (
	"gopkg.in/mgo.v2/bson"
	"reflect"
	"sync"
)

// ToValueChannel converts input channel to Value channel.
// The input may by raw []byte channel or Value channel (no conversion).
func ToValueChannel(input interface{}, concurrency int, bufferSize int) chan Value {
	if ch, ok := input.(chan Value); ok {
		return ch
	} else if ch, ok := input.(chan []byte); ok {
		outCh := make(chan Value, bufferSize)

		wg := &sync.WaitGroup{}
		wg.Add(concurrency)

		// Run workers
		for i := 0; i < concurrency; i++ {
			go func() {
				defer wg.Done()

				for bin := range ch {
					v := Value{}
					bson.Unmarshal(bin, &v)
					outCh <- v
				}
			}()
		}

		// Wait for workers
		go func() {
			wg.Wait()
			close(outCh)
		}()

		return outCh
	}

	panic("Invalid input. Expected 'chan expand.Value' or 'chan []byte'. Given: " + reflect.TypeOf(input).String())
}

// ValueChannelToSlice reads values from Value channel into slice.
func ValueChannelToSlice(ch chan Value) []interface{} {
	out := []interface{}{}
	for v := range ch {
		out = append(out, v)
	}
	return out
}
