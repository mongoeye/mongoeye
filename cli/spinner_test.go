package cli

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestRunWithSpinner_Buffer(t *testing.T) {
	out := bytes.NewBuffer(nil)

	ok := false

	RunWithSpinner(out, "Task:", func() {
		ok = true
	})

	assert.Equal(t, true, ok)
	assert.Equal(t, "Task: ...", out.String())
}

func TestRunWithSpinner_Stdout(t *testing.T) {
	ok := false

	RunWithSpinner(os.Stdout, "Task:", func() {
		ok = true
	})

	assert.Equal(t, true, ok)
}
