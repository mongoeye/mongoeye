package cli

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2"
	"sort"
	"testing"
	"time"
)

func TestPlans_Sort(t *testing.T) {
	plans := make(plans, 0)

	d1, _ := time.ParseDuration("21ms")
	d2, _ := time.ParseDuration("36ms")
	d3, _ := time.ParseDuration("14ms")
	p1 := &plan{TestDuration: d1}
	p2 := &plan{TestDuration: d2}
	p3 := &plan{TestDuration: d3}

	plans = append(plans, p1, p2, p3)
	sort.Sort(plans)

	assert.Equal(t, p3, plans[0])
	assert.Equal(t, p1, plans[1])
	assert.Equal(t, p2, plans[2])
}

func TestGenerateAnalysisPlans_UseAggregation(t *testing.T) {
	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{"cmd", "--use-aggregation"})

	config, _ := GetConfig(v)

	server := mgo.BuildInfo{
		Version:      "3.6.0",
		VersionArray: []int{3, 6, 0, 0},
	}

	plans := generateAnalysisPlans(server, 1, config)

	assert.Equal(t, 1, len(plans))
	assert.Equal(t, "db", plans[0].Name)
}

func TestGenerateAnalysisPlans_Local(t *testing.T) {
	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{"cmd"})

	config, _ := GetConfig(v)

	server := mgo.BuildInfo{
		Version:      "3.2.0",
		VersionArray: []int{3, 2, 0, 0},
	}

	plans := generateAnalysisPlans(server, 1, config)

	assert.Equal(t, 1, len(plans))
	assert.Equal(t, "local", plans[0].Name)
}

func TestGenerateAnalysisPlans_UserAggregation_InadequateVersion(t *testing.T) {
	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{"cmd", "--use-aggregation"})

	config, _ := GetConfig(v)

	server := mgo.BuildInfo{
		Version:      "3.2.0",
		VersionArray: []int{3, 2, 0, 0},
	}

	plans := generateAnalysisPlans(server, 1, config)
	assert.Equal(t, 0, len(plans))
}

func TestPlans_Limit(t *testing.T) {
	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{"cmd", "--sample", "first:20"})

	config, _ := GetConfig(v)

	server := mgo.BuildInfo{
		Version:      "3.6.0",
		VersionArray: []int{3, 6, 0, 0},
	}

	plans := generateAnalysisPlans(server, 30, config)
	sampleStage := plans[0].SampleStage
	pipeline := sampleStage.PipelineFactory(config.CreateAnalysisOptions()).GetStages()
	assert.Equal(t, 2, len(pipeline))
	assert.NotEqual(t, nil, pipeline[0]["$sort"])
	assert.NotEqual(t, nil, pipeline[1]["$limit"])
}

func TestPlans_LimitOptimization(t *testing.T) {
	cmd := &cobra.Command{}
	v := viper.New()
	InitFlags(cmd, v, "env")
	cmd.ParseFlags([]string{"cmd", "--sample", "first:20"})

	config, _ := GetConfig(v)

	server := mgo.BuildInfo{
		Version:      "3.6.0",
		VersionArray: []int{3, 6, 0, 0},
	}

	plans := generateAnalysisPlans(server, 10, config)
	sampleStage := plans[0].SampleStage
	pipeline := sampleStage.PipelineFactory(config.CreateAnalysisOptions()).GetStages()
	assert.Equal(t, 0, len(pipeline))
}
