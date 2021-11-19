package stats_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	stats "github.com/abayer/jenkins-usage-stats"
	"github.com/abayer/jenkins-usage-stats/testutil"
	testfixtures "github.com/go-testfixtures/testfixtures/v3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestReportFuncs(t *testing.T) {
	var getDBFunc func(*testing.T) (sq.BaseRunner, func())

	getDBFunc = dbWithFixtures
	// If USE_JENKINS_STATS_IT_DB is set, use the integration test DB rather than testcontainers-go.
	if os.Getenv("USE_JENKINS_STATS_IT_DB") != "" {
		getDBFunc = useITDB
	}

	// Pre-load the database with the fixtures we'll be using for the tests. We only do this once for all report func
	// tests because it takes ~45s to spin up the postgres container and load the fixtures into it on my beefy MBP.
	db, closeFunc := getDBFunc(t)
	defer closeFunc()

	// Make sure we have the same number of instance reports in the database that we do in the fixtures.
	var c int
	require.NoError(t, stats.PSQL(db).Select("count(*)").From(stats.InstanceReportsTable).QueryRow().Scan(&c))
	rawYaml, err := ioutil.ReadFile(filepath.Join("testdata", "fixtures", "instance_reports.yml"))
	require.NoError(t, err)
	var allYamlReports []interface{}
	require.NoError(t, yaml.Unmarshal(rawYaml, &allYamlReports))
	assert.Equal(t, len(allYamlReports), c)

	t.Run("GetInstallCountsForVersions", func(t *testing.T) {
		ir, err := stats.GetInstallCountForVersions(db, 2009, 12)
		require.NoError(t, err)

		goldenBytes := jsonReadGoldenAndUpdateIfDesired(t, ir)

		var goldenIR stats.InstallationReport
		require.NoError(t, json.Unmarshal(goldenBytes, &goldenIR))

		assert.Equal(t, goldenIR, ir)
	})

	t.Run("GetLatestPluginNumbers", func(t *testing.T) {
		pn, err := stats.GetLatestPluginNumbers(db, 2009, 12)
		require.NoError(t, err)

		goldenBytes := jsonReadGoldenAndUpdateIfDesired(t, pn)

		var goldenPN stats.LatestPluginNumbersReport
		require.NoError(t, json.Unmarshal(goldenBytes, &goldenPN))

		assert.Equal(t, goldenPN, pn)
	})

	t.Run("GetCapabilities", func(t *testing.T) {
		pn, err := stats.GetCapabilities(db, 2009, 12)
		require.NoError(t, err)

		goldenBytes := jsonReadGoldenAndUpdateIfDesired(t, pn)

		var goldenPN stats.CapabilitiesReport
		require.NoError(t, json.Unmarshal(goldenBytes, &goldenPN))

		assert.Equal(t, goldenPN, pn)
	})

	t.Run("JobCountsForMonth", func(t *testing.T) {
		pn, err := stats.JobCountsForMonth(db, 2009, 12)
		require.NoError(t, err)

		goldenBytes := jsonReadGoldenAndUpdateIfDesired(t, pn)

		var goldenPN map[string]uint64
		require.NoError(t, json.Unmarshal(goldenBytes, &goldenPN))

		assert.Equal(t, goldenPN, pn)
	})

	t.Run("OSCountsForMonth", func(t *testing.T) {
		pn, err := stats.OSCountsForMonth(db, 2009, 12)
		require.NoError(t, err)

		goldenBytes := jsonReadGoldenAndUpdateIfDesired(t, pn)

		var goldenPN map[string]uint64
		require.NoError(t, json.Unmarshal(goldenBytes, &goldenPN))

		assert.Equal(t, goldenPN, pn)
	})

	t.Run("GetJVMReports", func(t *testing.T) {
		pn, err := stats.GetJVMsReport(db, 2010, 2)
		require.NoError(t, err)

		goldenBytes := jsonReadGoldenAndUpdateIfDesired(t, pn)

		var goldenPN stats.JVMReport
		require.NoError(t, json.Unmarshal(goldenBytes, &goldenPN))

		assert.Equal(t, goldenPN, pn)
	})

	t.Run("GetPluginReports", func(t *testing.T) {
		pn, err := stats.GetPluginReports(db, 2010, 2)
		require.NoError(t, err)

		goldenBytes := jsonReadGoldenAndUpdateIfDesired(t, pn)

		var goldenPN []stats.PluginReport
		require.NoError(t, json.Unmarshal(goldenBytes, &goldenPN))

		assert.Equal(t, goldenPN, pn)
	})

	t.Run("JenkinsVersionsForPluginVersions", func(t *testing.T) {
		pn, err := stats.JenkinsVersionsForPluginVersions(db, 2010, 1)
		require.NoError(t, err)

		goldenBytes := jsonReadGoldenAndUpdateIfDesired(t, pn)

		var goldenPN map[string]map[string]map[string]uint64
		require.NoError(t, json.Unmarshal(goldenBytes, &goldenPN))

		assert.Equal(t, goldenPN, pn)
	})

	t.Run("ExecutorCountsForMonth", func(t *testing.T) {
		pn, err := stats.ExecutorCountsForMonth(db, 2010, 1)
		require.NoError(t, err)

		execSVG, execCSV, err := stats.CreateBarSVG(fmt.Sprintf("Executors per install (total: %d)", 5), pn, 25, false, false, true, stats.DefaultFilter)
		require.NoError(t, err)

		goldenSVG := rawReadGoldenAndUpdateIfDesired(t, execSVG, "svg")
		assert.Equal(t, string(goldenSVG), string(execSVG))

		goldenCSV := rawReadGoldenAndUpdateIfDesired(t, execCSV, "csv")
		assert.Equal(t, string(goldenCSV), string(execCSV))
	})

	t.Run("GenerateReport", func(t *testing.T) {
		tmpOut, err := os.MkdirTemp("", "out-dir-")
		require.NoError(t, err)
		defer func() {
			_ = os.RemoveAll(tmpOut)
		}()

		require.NoError(t, stats.GenerateReport(db, 2010, 2, tmpOut))
	})
}

func jsonReadGoldenAndUpdateIfDesired(t *testing.T, input interface{}) []byte {
	testName := strings.Split(t.Name(), "/")[1]

	goldenFile := filepath.Join("testdata", "reports", fmt.Sprintf("%s.json", testName))

	if os.Getenv("UPDATE_GOLDEN") != "" {
		jb, err := json.MarshalIndent(input, "", "  ")
		require.NoError(t, err)
		require.NoError(t, ioutil.WriteFile(goldenFile, jb, 0644)) //nolint:gosec
	}

	goldenBytes, err := ioutil.ReadFile(goldenFile) //nolint:gosec
	require.NoError(t, err)

	return goldenBytes
}

func rawReadGoldenAndUpdateIfDesired(t *testing.T, input []byte, suffix string) []byte {
	testName := strings.Split(t.Name(), "/")[1]

	goldenFile := filepath.Join("testdata", "reports", fmt.Sprintf("%s.%s", testName, suffix))

	if os.Getenv("UPDATE_GOLDEN") != "" {
		require.NoError(t, ioutil.WriteFile(goldenFile, input, 0644)) //nolint:gosec
	}

	goldenBytes, err := ioutil.ReadFile(goldenFile) //nolint:gosec
	require.NoError(t, err)

	return goldenBytes
}

func dbWithFixtures(t *testing.T) (sq.BaseRunner, func()) {
	db, closeFunc := testutil.DBForTest(t)
	fixtures, err := testfixtures.New(
		testfixtures.Database(db),
		testfixtures.Dialect("postgres"),
		testfixtures.Directory(filepath.Join("testdata", "fixtures")),
		// Make sure we don't inadvertently bork sequences
		testfixtures.ResetSequencesTo(30000),
		// We store timestamps in UTC
		testfixtures.Location(time.UTC))
	if err != nil {
		closeFunc()
		t.Fatal(err)
	}

	err = fixtures.Load()
	if err != nil {
		closeFunc()
		t.Fatal(err)
	}

	return sq.NewStmtCacheProxy(db), closeFunc
}

func useITDB(t *testing.T) (sq.BaseRunner, func()) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres@localhost/jenkins_usage_stats_test?sslmode=disable&timezone=UTC"
	}

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		t.Fatal(err)
	}

	closeFunc := func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}

	return sq.NewStmtCacheProxy(db), closeFunc
}
