package cli

import (
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFlags_AddSection(t *testing.T) {
	flags := &Flags{}
	s := flags.AddSection("newSection")
	assert.NotEqual(t, nil, s)
	assert.Equal(t, 1, len(flags.Sections))
	assert.Equal(t, s, flags.Sections[0])
}

func TestFlags_ExportAll(t *testing.T) {
	flags := &Flags{}
	s1 := flags.AddSection("newSection1")
	s1.Set.String("flag1", "value1", "usage1")
	s1.Set.String("flag2", "value2", "usage2")

	s2 := flags.AddSection("newSection2")
	s2.Set.String("flag3", "value3", "usage3")
	s2.Set.String("flag4", "value4", "usage4")

	set := &pflag.FlagSet{}
	flags.ExportAll(set)

	names := make([]string, 0)
	set.VisitAll(func(flag *pflag.Flag) {
		names = append(names, flag.Name)
	})

	assert.Equal(t, names, []string{"flag1", "flag2", "flag3", "flag4"})
}
