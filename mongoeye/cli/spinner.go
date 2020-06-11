package cli

import (
	"fmt"
	"github.com/andrew-d/go-termutil"
	"github.com/briandowns/spinner"
	"io"
	"os"
	"sync"
	"time"
)

// RunWithSpinner allows run task with showing the spinner.
func RunWithSpinner(out io.Writer, msg string, task func()) {
	// Print info message
	fmt.Fprintf(out, "%s ", msg)

	// Run spinner if interactive terminal
	var s *spinner.Spinner
	if f, ok := out.(*os.File); ok && f.Name() == "/dev/stdout" && termutil.Isatty(os.Stdin.Fd()) {
		s = spinner.New(spinner.CharSets[24], 75*time.Millisecond)
		s.Start()
		defer s.Stop()
	} else {
		fmt.Fprint(out, "...")
	}

	// Run task
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		task()
	}()

	// Wait for task
	wg.Wait()
}
