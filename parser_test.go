package stats_test

import (
	"path/filepath"
	"testing"
	"time"

	stats "github.com/abayer/jenkins-usage-stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDailyJSON(t *testing.T) {
	fooFile := filepath.Join("testdata", "base.json.gz")

	reports, err := stats.ParseDailyJSON(fooFile)
	require.NoError(t, err)
	assert.Len(t, reports, 2)
	assert.Equal(t, "32b68faa8644852c4ad79540b4bfeb1caf63284811f4f9d6c2bc511f797218c8", reports[0].Install)
	assert.Equal(t, uint64(50), reports[0].Jobs["hudson-maven-MavenModuleSet"])
	assert.Len(t, reports[0].Plugins, 75)
	assert.Equal(t, "1.8", reports[0].Nodes[0].JVMVersion)
	assert.Equal(t, "1.6", reports[1].Nodes[0].JVMVersion)

	ts, err := reports[0].Timestamp()
	require.NoError(t, err)
	assert.Equal(t, time.Date(2021, time.October, 30, 23, 59, 54, 0, time.UTC), ts)
}

func TestFilterPrivateFromReport(t *testing.T) {
	report := &stats.JSONReport{
		Plugins: []stats.JSONPlugin{
			{
				Name:    "legit-plugin",
				Version: "1.2.3",
			},
			{
				Name:    "privateplugin-something",
				Version: "1.2.3",
			},
			{
				Name:    "other-legit-plugin",
				Version: "2.3.4 (private)",
			},
			{
				Name:    "final-legit-plugin",
				Version: "4.5.6",
			},
		},
	}

	stats.FilterPrivateFromReport(report)

	assert.Len(t, report.Plugins, 2)
	assert.Equal(t, report.Plugins[0].Name, "legit-plugin")
	assert.Equal(t, report.Plugins[1].Name, "final-legit-plugin")
}
