package cli

import (
	"bytes"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestFlags_Usage(t *testing.T) {
	flags := &Flags{}
	s1 := flags.AddSection("section A")
	s1.Set.String("flag1", "value1", "usage1")
	s1.Set.String("flag2", "value2", "usage2")

	s2 := flags.AddSection("section B")
	s2.Set.String("flag3", "value3", "usage3")
	s2.Set.String("flag4", "value4", "usage4")

	expected := `  section A:
        --flag1  usage1 (default "value1")
        --flag2  usage2 (default "value2")

  section B:
        --flag3  usage3 (default "value3")
        --flag4  usage4 (default "value4")
`

	assert.Equal(t, flags.Usage(), expected)
}

func TestFlags_CmdUsage(t *testing.T) {
	os.Clearenv()

	cmd := &cobra.Command{
		Short: "short info",
		Long:  "long info",
		Use:   "use",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprint(cmd.OutOrStdout(), "run!\n")
		},
	}
	v := viper.New()
	InitFlags(cmd, v, "xyz")

	out := new(bytes.Buffer)
	cmd.SetOutput(out)
	cmd.ParseFlags([]string{"-h"})
	cmd.Execute()

	outStr := out.String()

	assert.Contains(t, outStr, "long info\n")
	assert.Contains(t, outStr, "Usage:\n")
	assert.Contains(t, outStr, "use [flags]")
	assert.Contains(t, outStr, "Flags:\n")
	assert.Contains(t, outStr, "  connection options:\n")
	assert.Contains(t, outStr, "        --host")
}
