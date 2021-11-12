package pkg_test

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/abayer/jenkins-usage-stats/pkg"
	"github.com/abayer/jenkins-usage-stats/pkg/testutil"
	"github.com/go-testfixtures/testfixtures/v3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestReportFuncs(t *testing.T) {
	// Pre-load the database with the fixtures we'll be using for the tests. We only do this once for all report func
	// tests because it takes ~45s to spin up the postgres container and load the fixtures into it on my beefy MBP.
	db, closeFunc := dbWithFixtures(t)
	defer closeFunc()

	// Make sure we have the same number of instance reports in the database that we do in the fixtures.
	var c int
	require.NoError(t, pkg.PSQL(db).Select("count(*)").From(pkg.InstanceReportsTable).QueryRow().Scan(&c))
	rawYaml, err := ioutil.ReadFile(filepath.Join("testdata", "fixtures", "instance_reports.yml"))
	require.NoError(t, err)
	var allYamlReports []interface{}
	require.NoError(t, yaml.Unmarshal(rawYaml, &allYamlReports))
	assert.Equal(t, len(allYamlReports), c)

	t.Run("GetInstallCountsForVersions", func(t *testing.T) {
		ir, err := pkg.GetInstallCountForVersions(db, 2009, 12)
		require.NoError(t, err)

		goldenFile := filepath.Join("testdata", "reports", "getInstallCountForVersions.json")
		goldenBytes := readGoldenAndUpdateIfDesired(t, goldenFile, ir)

		var goldenIR pkg.InstallationReport
		require.NoError(t, json.Unmarshal(goldenBytes, &goldenIR))

		assert.Equal(t, goldenIR, ir)
	})

	t.Run("GetLatestPluginNumbers", func(t *testing.T) {
		pn, err := pkg.GetLatestPluginNumbers(db, 2009, 12)
		require.NoError(t, err)

		goldenFile := filepath.Join("testdata", "reports", "getLatestPluginNumbers.json")
		goldenBytes := readGoldenAndUpdateIfDesired(t, goldenFile, pn)

		var goldenPN pkg.LatestPluginNumbersReport
		require.NoError(t, json.Unmarshal(goldenBytes, &goldenPN))

		assert.Equal(t, goldenPN, pn)

	})
}

func readGoldenAndUpdateIfDesired(t *testing.T, goldenFile string, input interface{}) []byte {
	if os.Getenv("UPDATE_GOLDEN") != "" {
		jb, err := json.MarshalIndent(input, "", "  ")
		require.NoError(t, err)
		require.NoError(t, ioutil.WriteFile(goldenFile, jb, 0644)) //nolint:gosec
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

	return db, closeFunc
}
