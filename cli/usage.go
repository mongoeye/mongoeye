package cli

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/spf13/cobra"
	"regexp"
	"strings"
	"text/template"
	"unicode"
)

// Usage generates usage information of flags.
func (f *Flags) Usage() string {
	out := new(bytes.Buffer)

	// Match original usage parts
	e := regexp.MustCompile(`^((.*--[^\s]+)(\s[^\s]+)?)(\s\s+)(.*)$`)

	data := make([][][]string, 0)

	maxLength := 0
	for _, s := range f.Sections {
		scanner := bufio.NewScanner(strings.NewReader(s.Set.FlagUsages()))

		parts := make([][]string, 0)

		for scanner.Scan() {
			p := e.FindStringSubmatch(scanner.Text())
			l := len(p[2])
			parts = append(parts, p)

			if maxLength < l {
				maxLength = l
			}
		}

		data = append(data, parts)
	}

	for i, s := range f.Sections {
		if i > 0 {
			out.WriteString("\n")
		}

		parts := data[i]
		out.WriteString(fmt.Sprintf("  %s:\n", s.Name))
		for _, p := range parts {
			out.WriteString(fmt.Sprintf("  %s%s  %s\n",
				p[2],
				strings.Repeat(" ", maxLength-len(p[2])),
				p[5],
			))
		}
	}

	return out.String()
}

func usageFunc(flags *Flags) func(c *cobra.Command) error {
	return func(c *cobra.Command) error {
		w := c.OutOrStderr()
		t := template.New("usage")
		t.Funcs(template.FuncMap{
			"trim": strings.TrimSpace,
			"trimTrailingWhitespaces": func(s string) string {
				return strings.TrimRightFunc(s, unicode.IsSpace)
			},
		})

		template.Must(t.Parse(
			`Usage:
  {{.Cmd.UseLine}}

Flags:
{{.Flags.Usage | trimTrailingWhitespaces}}

Note: You can also use environment variables, eg. 'export MONGOEYE_COUNT-UNIQUE=true'
`))

		err := t.Execute(w, struct {
			Cmd   *cobra.Command
			Flags *Flags
		}{
			Cmd:   c,
			Flags: flags,
		})

		if err != nil {
			c.Println(err)
		}

		return err
	}
}
