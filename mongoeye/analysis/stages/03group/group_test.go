package group

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOptions_IsNecessaryToCalcValueFreq(t *testing.T) {
	var o *Options

	o = &Options{}
	assert.Equal(t, false, o.IsNecessaryToCalcValueFreq())

	o = &Options{StoreCountOfUnique: true}
	assert.Equal(t, true, o.IsNecessaryToCalcValueFreq())

	o = &Options{StoreMostFrequent: 10}
	assert.Equal(t, true, o.IsNecessaryToCalcValueFreq())

	o = &Options{StoreLeastFrequent: 10}
	assert.Equal(t, true, o.IsNecessaryToCalcValueFreq())

	o = &Options{ValueHistogramMaxRes: 100}
	assert.Equal(t, true, o.IsNecessaryToCalcValueFreq())
}

func TestOptions_IsNecessaryToCalcLengthFreq(t *testing.T) {
	var o *Options

	o = &Options{}
	assert.Equal(t, false, o.IsNecessaryToCalcLengthFreq())

	o = &Options{LengthHistogramMaxRes: 100}
	assert.Equal(t, true, o.IsNecessaryToCalcLengthFreq())
}
