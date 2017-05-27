package cli

import (
	"bytes"
	"fmt"
	"github.com/fatih/color"
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/olekukonko/tablewriter"
	"runtime"
	"strconv"
	"strings"
)

const allDocumentsTitle = "all documents"
const analyzedDocumentsTitle = "analyzed documents"

type style struct {
	line      func(a ...interface{}) string
	key       func(a ...interface{}) string
	infoKey   func(a ...interface{}) string
	arrayItem func(a ...interface{}) string
	typeName  func(a ...interface{}) string
	typeArrow func(a ...interface{}) string
	count     func(a ...interface{}) string
	typeCount func(a ...interface{}) string
	pct       func(a ...interface{}) string
	typePct   func(a ...interface{}) string

	objectName  func(a ...interface{}) string
	objectCount func(a ...interface{}) string
}

type symbols struct {
	typeArrow     string
	lineCommon    string
	lineLast      string
	lineEndCommon string
	lineEndMiddle string
	lineEndLastS  string
	lineEndLastM  string
}

type formatFunc struct {
	count func(count uint64) string
	pct   func(a uint64, b uint64) string
}

// TableFormatter contains the data needed to draw the results as a table.
type TableFormatter struct {
	style    style
	symbols  symbols
	format   formatFunc
	out      *bytes.Buffer
	table    *tablewriter.Table
	countMap map[string]uint64
}

// NewTableFormatter creates TableFormatter.
func NewTableFormatter(colorOutput bool) *TableFormatter {
	formatter := &TableFormatter{
		symbols: symbols{
			typeArrow:     "➜ ",
			lineCommon:    "│ ",
			lineLast:      "├╴",
			lineEndCommon: "└─",
			lineEndMiddle: "┴─",
			lineEndLastS:  "└╴",
			lineEndLastM:  "┴╴",
		},
	}

	if runtime.GOOS == "windows" {
		formatter.symbols.typeArrow = "- "
	}

	if colorOutput {
		formatter.style = style{
			line:      color.New(color.FgCyan).SprintFunc(),
			key:       color.New(color.Bold, color.FgCyan).SprintFunc(),
			infoKey:   color.New(color.Bold).SprintFunc(),
			arrayItem: color.New(color.Italic, color.FgCyan).SprintFunc(),
			typeName:  color.New(color.FgGreen).SprintFunc(),
			typeArrow: color.New().SprintFunc(),
			count:     color.New(color.Bold).SprintFunc(),
			typeCount: color.New(color.FgGreen).SprintFunc(),
			pct:       color.New(color.Bold).SprintFunc(),
			typePct:   color.New(color.FgGreen).SprintFunc(),

			objectName:  color.New(color.FgYellow).SprintFunc(),
			objectCount: color.New(color.Bold, color.FgYellow).SprintFunc(),
		}
	} else {
		noOp := func(a ...interface{}) string {
			return fmt.Sprint(a...)
		}
		formatter.style = style{
			line:      noOp,
			infoKey:   noOp,
			key:       noOp,
			arrayItem: noOp,
			typeName:  noOp,
			typeArrow: noOp,
			count:     noOp,
			typeCount: noOp,
			pct:       noOp,
			typePct:   noOp,

			objectName:  noOp,
			objectCount: noOp,
		}
	}

	formatter.out = bytes.NewBuffer(nil)
	formatter.table = tablewriter.NewWriter(formatter.out)
	formatter.table.SetBorder(false)
	formatter.table.SetAutoWrapText(false)
	formatter.table.SetAlignment(tablewriter.ALIGN_LEFT)
	formatter.table.SetRowSeparator("─")
	formatter.table.SetColumnSeparator("│")
	formatter.table.SetCenterSeparator("─")
	formatter.table.SetHeader([]string{"KEY", "COUNT ", "%"})

	return formatter
}

// RenderResults renders results of analysis as a table.
func (f *TableFormatter) RenderResults(result *Result) []byte {
	// Format count
	f.countMap = map[string]uint64{"": result.DocsCount}
	countFormat := fmt.Sprintf("%%%dd", len(strconv.Itoa(int(result.AllDocsCount))))
	f.format.count = func(count uint64) string {
		return fmt.Sprintf(countFormat, count)
	}

	// Format percentage
	f.format.pct = func(a uint64, b uint64) string {
		return fmt.Sprintf("%5.1f", (float64(a)/float64(b))*float64(100))
	}

	// All documents count
	f.table.Append([]string{
		f.style.infoKey(allDocumentsTitle),
		f.style.count(f.format.count(result.AllDocsCount)),
		"",
	})

	// Processed documents count
	f.table.Append([]string{
		f.style.infoKey(analyzedDocumentsTitle),
		f.style.objectCount(f.format.count(result.DocsCount)),
		f.style.pct(f.format.pct(result.DocsCount, result.AllDocsCount)),
	})

	// Space
	f.table.Append([]string{"", "", ""})

	// Append fields
	var previous *analysis.Field
	var next *analysis.Field
	for i := uint64(0); i < result.FieldsCount; i++ {
		// Previous
		if i > 0 {
			previous = result.Fields[i-1]
		} else {
			previous = nil
		}
		// Next
		if i+1 < result.FieldsCount {
			next = result.Fields[i+1]
		} else {
			next = nil
		}
		f.processField(previous, result.Fields[i], next)
	}

	f.table.Render()
	return f.out.Bytes()
}

func (f *TableFormatter) processField(previous *analysis.Field, field *analysis.Field, next *analysis.Field) {
	// Fields are sorted so that the parent field is processed first
	f.countMap[field.Name] = field.Count

	// Get short field name and parent count
	parts := strings.Split(field.Name, analysis.NameSeparator)
	l := len(parts)
	shortKey := parts[l-1]
	parentKey := strings.Join(parts[:(l-1)], analysis.NameSeparator)
	parentCount := f.countMap[parentKey]

	f.appendFieldRow(previous, field, next, shortKey, parentCount)
	if len(field.Types) > 1 {
		f.appendTypeRows(previous, field, next, field.Count)
	}
}

func (f *TableFormatter) appendFieldRow(previous *analysis.Field, field *analysis.Field, next *analysis.Field, shortKey string, parentCount uint64) {
	countStr := f.style.count(f.format.count(field.Count))

	// If the field contains only one type, it is written directly with the name
	typeStr := ""
	if len(field.Types) == 1 {
		typeNameStr := f.style.typeName(field.Types[0].Name)
		if field.Types[0].Name == "object" {
			typeNameStr = f.style.objectName(field.Types[0].Name)
			countStr = f.style.objectCount(f.format.count(field.Count))
		}

		typeStr = fmt.Sprintf(
			"%s%s",
			f.style.typeArrow(" "+f.symbols.typeArrow),
			typeNameStr,
		)
	}

	// Array item is printed differently
	keyStr := f.style.key(shortKey)
	pctStr := f.style.pct(f.format.pct(field.Count, parentCount))
	if shortKey == analysis.ArrayItemMark {
		keyStr = f.style.arrayItem("[array item]")
		pctStr = ""
	}

	// Append field name, count and percentage
	f.table.Append([]string{
		fmt.Sprintf("%s%s%s",
			f.style.line(f.generateFieldLine(previous, field, next)),
			keyStr,
			typeStr,
		),
		countStr,
		pctStr,
	})
}

func (f *TableFormatter) appendTypeRows(previous *analysis.Field, field *analysis.Field, next *analysis.Field, fieldCount uint64) {
	for i, t := range field.Types {
		typeStr := f.style.typeName(t.Name)
		countStr := f.style.typeCount(f.format.count(t.Count))

		// Object type
		if t.Name == "object" {
			// Only object type is parent of sub fields
			f.countMap[field.Name] = t.Count
			typeStr = f.style.objectName(t.Name)
			countStr = f.style.objectCount(f.format.count(t.Count))
		}

		// Append type name, count and percentage
		f.table.Append([]string{
			fmt.Sprintf("%s%s%s",
				f.style.line(f.generateTypeLine(previous, field, next, i+1, t)),
				f.symbols.typeArrow,
				typeStr,
			),
			countStr,
			f.style.typePct(f.format.pct(t.Count, fieldCount)),
		})
	}
}

func (f *TableFormatter) generateFieldLine(previous *analysis.Field, field *analysis.Field, next *analysis.Field) string {
	// Line end bound
	levelDiff := int(field.Level)
	bound := uint(0)
	if next != nil {
		bound = field.Level
		if len(field.Types) == 1 {
			levelDiff = int(field.Level) - int(next.Level)
			bound = next.Level
		}
	}

	b := bytes.NewBuffer(nil)
	for i := uint(1); i <= field.Level; i++ {
		if i > bound {
			// End of line
			if i == field.Level {
				if levelDiff > 1 {
					b.WriteString(f.symbols.lineEndLastM)
				} else {
					b.WriteString(f.symbols.lineEndLastS)
				}
			} else if i == bound+1 {
				b.WriteString(f.symbols.lineEndCommon)
			} else {
				b.WriteString(f.symbols.lineEndMiddle)
			}
		} else {
			// Continuation of the previous line
			if i == field.Level {
				b.WriteString(f.symbols.lineLast)
			} else {
				b.WriteString(f.symbols.lineCommon)
			}
		}
	}

	return b.String()
}

func (f *TableFormatter) generateTypeLine(previous *analysis.Field, field *analysis.Field, next *analysis.Field, typePos int, t *analysis.Type) string {
	last := typePos == len(field.Types)

	// Line end bound
	levelDiff := int(field.Level + 1)
	bound := int(0)
	if next != nil {
		levelDiff = int(field.Level+1) - int(next.Level)
		bound = int(field.Level)
		if last && levelDiff > 0 {
			bound = int(next.Level) - 1
		}
	}

	b := bytes.NewBuffer(nil)
	for i := uint(0); i <= field.Level; i++ {
		if last && int(i) > bound {
			if levelDiff > 1 {
				if i == field.Level {
					b.WriteString(f.symbols.lineEndLastM)
					continue
				} else if int(i) == bound+1 {
					b.WriteString(f.symbols.lineEndCommon)
					continue
				} else {
					b.WriteString(f.symbols.lineEndMiddle)
					continue
				}
			} else if i == field.Level {
				b.WriteString(f.symbols.lineEndLastS)
				continue
			}
		}

		b.WriteString(f.symbols.lineCommon)
	}

	return b.String()
}
