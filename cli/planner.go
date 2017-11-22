package cli

import (
	"github.com/mongoeye/mongoeye/analysis"
	"github.com/mongoeye/mongoeye/analysis/stages/01sample"
	"github.com/mongoeye/mongoeye/analysis/stages/01sample/sampleInDB"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand/expandInDBDepth"
	"github.com/mongoeye/mongoeye/analysis/stages/02expand/expandLocally"
	"github.com/mongoeye/mongoeye/analysis/stages/03group/groupInDB"
	"github.com/mongoeye/mongoeye/analysis/stages/03group/groupLocally"
	"github.com/mongoeye/mongoeye/analysis/stages/04merge"
	"github.com/mongoeye/mongoeye/analysis/stages/04merge/mergeInDB"
	"github.com/mongoeye/mongoeye/analysis/stages/04merge/mergeLocally"
	"gopkg.in/mgo.v2"
	"runtime"
	"sort"
	"time"
)

// Plans of analysis.
type plans []*plan

func (p plans) Len() int           { return len(p) }
func (p plans) Less(i, j int) bool { return p[i].TestDuration < p[j].TestDuration }
func (p plans) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Plan of analysis.
type plan struct {
	Name         string
	Config       *Config
	Options      *analysis.Options
	SampleStage  *analysis.Stage
	ExpandStage  *analysis.Stage
	GroupStage   *analysis.Stage
	MergeStage   *analysis.Stage
	AllDocsCount uint64
	Limit        uint64
	TestDuration time.Duration
}

func (p *plan) Run(collection *mgo.Collection) Result {
	runtime.GOMAXPROCS(p.Options.Concurrency)

	a := analysis.NewAnalysis(p.Options)
	a.SetSampleStage(p.SampleStage)
	a.SetExpandStage(p.ExpandStage)
	a.SetGroupStage(p.GroupStage)
	a.SetMergeStage(p.MergeStage)
	a.SetCollection(collection)

	start := time.Now()
	ch := merge.ToFieldChannel(a.Run(), p.Options.Location, p.Options.Concurrency, p.Options.BufferSize)
	fields := merge.FieldChannelToSlice(ch)
	duration := time.Since(start)

	sort.Sort(fields)

	analyzedDocs := p.Limit
	if analyzedDocs == 0 {
		analyzedDocs = p.AllDocsCount
	}

	return Result{
		Database:     p.Config.Database,
		Collection:   p.Config.Collection,
		Plan:         p.Name,
		Duration:     duration,
		AllDocsCount: p.AllDocsCount,
		DocsCount:    analyzedDocs,
		FieldsCount:  uint64(len(fields)),
		Fields:       fields,
	}
}

func generateAnalysisPlans(server mgo.BuildInfo, count int, config *Config) plans {
	// Analysis method
	runAggregation := false
	runLocal := true
	if config.UseAggregation {
		runAggregation = true
		runLocal = false
	}

	// Options for individual stages
	analysisOptions := config.CreateAnalysisOptions()
	sampleOptions := config.CreateSampleStageOptions()
	expandOptions := config.CreateExpandStageOptions()
	groupOptions := config.CreateGroupStageOptions()
	mergeOptions := config.CreateMergeStageOptions()

	// Optimize sample stage
	if sampleOptions.Method != sample.AllDocuments && sampleOptions.Limit > uint64(count) {
		sampleOptions.Method = sample.AllDocuments
		sampleOptions.Limit = 0
	}

	// Possible  plans
	plans := make(plans, 0)

	if runLocal {
		plans = append(plans, &plan{
			Name:         "local",
			Config:       config,
			Options:      analysisOptions,
			SampleStage:  sampleInDB.NewStage(sampleOptions),
			ExpandStage:  expandLocally.NewStage(expandOptions),
			GroupStage:   groupLocally.NewStage(groupOptions),
			MergeStage:   mergeLocally.NewStage(mergeOptions),
			AllDocsCount: uint64(count),
			Limit:        sampleOptions.Limit,
		})
	}

	if runAggregation && server.VersionAtLeast(analysis.AggregationMinVersion...) {
		plans = append(plans, &plan{
			Name:         "db",
			Config:       config,
			Options:      analysisOptions,
			SampleStage:  sampleInDB.NewStage(sampleOptions),
			ExpandStage:  expandInDBDepth.NewStage(expandOptions),
			GroupStage:   groupInDB.NewStage(groupOptions),
			MergeStage:   mergeInDB.NewStage(mergeOptions),
			AllDocsCount: uint64(count),
			Limit:        sampleOptions.Limit,
		})
	}

	return plans
}
