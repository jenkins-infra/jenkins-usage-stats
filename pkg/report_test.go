package pkg_test

import (
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/abayer/jenkins-usage-stats/pkg"
	"github.com/abayer/jenkins-usage-stats/pkg/testutil"
	"github.com/go-testfixtures/testfixtures/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestReportFuncs(t *testing.T) {
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

}

func dbWithFixtures(t *testing.T) (sq.BaseRunner, func()) {
	db, closeFunc := testutil.DBForTest(t)

	fixtures, err := testfixtures.New(
		testfixtures.Database(db),
		testfixtures.Dialect("postgres"),
		testfixtures.Directory("testdata/fixtures"),
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
