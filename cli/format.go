package cli

import (
	"encoding/json"
	"github.com/mongoeye/mongoeye/analysis"
	"gopkg.in/yaml.v2"
	"time"
)

// Result of analysis.
type Result struct {
	Database     string          `json:"database"         yaml:"database"`
	Collection   string          `json:"collection"       yaml:"collection"`
	Plan         string          `json:"plan"             yaml:"plan"`
	Duration     time.Duration   `json:"duration"         yaml:"duration"`
	AllDocsCount uint64          `json:"allDocs"          yaml:"allDocs"`
	DocsCount    uint64          `json:"analyzedDocs"     yaml:"analyzedDocs"`
	FieldsCount  uint64          `json:"fieldsCount"      yaml:"fieldsCount"`
	Fields       analysis.Fields `json:"fields"           yaml:"fields"`
}

// Format result of analysis.
func Format(result Result, config *Config) ([]byte, error) {
	switch config.Format {
	case "table":
		return formatTable(result, config)
	case "json":
		return formatJson(result, config)
	case "yaml":
		return formatYaml(result, config)
	default:
		panic("Unexpected format.")
	}
}

func formatJson(result Result, config *Config) (out []byte, err error) {
	// JSON output is pretty printed to console and compressed to file
	if config.FilePath == "" {
		out, err = json.MarshalIndent(result, "", "\t")
	} else {
		out, err = json.Marshal(result)
	}

	return
}

func formatYaml(result Result, config *Config) (out []byte, err error) {
	out, err = yaml.Marshal(result)
	return
}

func formatTable(result Result, config *Config) ([]byte, error) {
	color := !config.NoColor && config.FilePath == ""
	table := NewTableFormatter(color)
	return table.RenderResults(&result), nil
}
