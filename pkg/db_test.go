package pkg_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sq "github.com/Masterminds/squirrel"
	"github.com/abayer/jenkins-usage-stats/pkg"
)

func TestGetJVMVersionID(t *testing.T) {
	db, closeFunc := DBForTest(t)
	defer closeFunc()

	cache := pkg.NewStatsCache()
	firstVer := "1.7"
	secondVer := "13"

	var fetchedVersion pkg.JVMVersion
	err := pkg.PSQL(db).Select("id", "name").From(pkg.JVMVersionsTable).Where(sq.Eq{"name": firstVer}).
		QueryRow().Scan(&fetchedVersion.ID, &fetchedVersion.Name)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := pkg.GetJVMVersionID(db, cache, firstVer)
	require.NoError(t, err)
	require.NoError(t, pkg.PSQL(db).Select("id", "name").From(pkg.JVMVersionsTable).Where(sq.Eq{"name": firstVer}).
		QueryRow().Scan(&fetchedVersion.ID, &fetchedVersion.Name))
	assert.Equal(t, firstID, fetchedVersion.ID)

	secondID, err := pkg.GetJVMVersionID(db, cache, secondVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, secondID)
}

func TestGetOSTypeID(t *testing.T) {
	db, closeFunc := DBForTest(t)
	defer closeFunc()

	cache := pkg.NewStatsCache()

	firstVer := "Windows 11"
	secondVer := "Ubuntu something"

	var fetchedOS pkg.OSType
	err := pkg.PSQL(db).Select("id", "name").From(pkg.OSTypesTable).Where(sq.Eq{"name": firstVer}).
		QueryRow().Scan(&fetchedOS.ID, &fetchedOS.Name)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := pkg.GetOSTypeID(db, cache, firstVer)
	require.NoError(t, err)
	require.NoError(t, pkg.PSQL(db).Select("id", "name").From(pkg.OSTypesTable).Where(sq.Eq{"name": firstVer}).
		QueryRow().Scan(&fetchedOS.ID, &fetchedOS.Name))
	assert.Equal(t, firstID, fetchedOS.ID)

	secondID, err := pkg.GetOSTypeID(db, cache, secondVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, secondID)
}

func TestGetJobTypeID(t *testing.T) {
	db, closeFunc := DBForTest(t)
	defer closeFunc()

	cache := pkg.NewStatsCache()

	firstVer := "hudson-maven-MavenModuleSet"
	secondVer := "org-jenkinsci-plugins-workflow-job-WorkflowJob"

	var fetchedJobType pkg.JobType
	err := pkg.PSQL(db).Select("id", "name").From(pkg.JobTypesTable).Where(sq.Eq{"name": firstVer}).
		QueryRow().Scan(&fetchedJobType.ID, &fetchedJobType.Name)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := pkg.GetJobTypeID(db, cache, firstVer)
	require.NoError(t, err)
	require.NoError(t, pkg.PSQL(db).Select("id", "name").From(pkg.JobTypesTable).Where(sq.Eq{"name": firstVer}).
		QueryRow().Scan(&fetchedJobType.ID, &fetchedJobType.Name))
	assert.Equal(t, firstID, fetchedJobType.ID)

	secondID, err := pkg.GetJobTypeID(db, cache, secondVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, secondID)
}

func TestGetJenkinsVersionID(t *testing.T) {
	db, closeFunc := DBForTest(t)
	defer closeFunc()

	cache := pkg.NewStatsCache()

	firstVer := "1.500"
	secondVer := "2.201.1"

	var fetchedJV pkg.JenkinsVersion
	err := pkg.PSQL(db).Select("id", "version").From(pkg.JenkinsVersionsTable).Where(sq.Eq{"version": firstVer}).
		QueryRow().Scan(&fetchedJV.ID, &fetchedJV.Version)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := pkg.GetJenkinsVersionID(db, cache, firstVer)
	require.NoError(t, err)
	require.NoError(t, pkg.PSQL(db).Select("id", "version").From(pkg.JenkinsVersionsTable).Where(sq.Eq{"version": firstVer}).
		QueryRow().Scan(&fetchedJV.ID, &fetchedJV.Version))
	assert.Equal(t, firstID, fetchedJV.ID)

	secondID, err := pkg.GetJenkinsVersionID(db, cache, secondVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, secondID)
}

func TestGetPluginID(t *testing.T) {
	db, closeFunc := DBForTest(t)
	defer closeFunc()

	cache := pkg.NewStatsCache()

	firstName := "first-plugin"
	firstVer := "1.0"
	secondVer := "2.0"
	secondName := "second-plugin"

	var fetchedPlugin pkg.Plugin
	err := pkg.PSQL(db).Select("id", "name", "version").From(pkg.PluginsTable).Where(sq.Eq{"name": firstName}).Where(sq.Eq{"version": firstVer}).
		QueryRow().Scan(&fetchedPlugin.ID, &fetchedPlugin.Name, &fetchedPlugin.Version)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := pkg.GetPluginID(db, cache, firstName, firstVer)
	require.NoError(t, err)
	require.NoError(t, pkg.PSQL(db).Select("id", "name", "version").From(pkg.PluginsTable).Where(sq.Eq{"name": firstName}).Where(sq.Eq{"version": firstVer}).
		QueryRow().Scan(&fetchedPlugin.ID, &fetchedPlugin.Name, &fetchedPlugin.Version))
	assert.Equal(t, firstID, fetchedPlugin.ID)

	secondID, err := pkg.GetPluginID(db, cache, firstName, secondVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, secondID)

	otherPluginID, err := pkg.GetPluginID(db, cache, secondName, firstVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, otherPluginID)
}

func TestAddIndividualReport(t *testing.T) {
	db, closeFunc := DBForTest(t)
	defer closeFunc()

	cache := pkg.NewStatsCache()

	initialFile := filepath.Join("testdata", "base.json.gz")
	jsonReports, err := pkg.ParseDailyJSON(initialFile)
	require.NoError(t, err)

	for _, jr := range jsonReports {
		require.NoError(t, pkg.AddIndividualReport(db, cache, jr))
	}

	result, err := pkg.PSQL(db).Select("count(*)").
		From(pkg.InstanceReportsTable).
		Query()
	require.NoError(t, err)

	var counts []int
	for result.Next() {
		var c int
		require.NoError(t, result.Scan(&c))
		counts = append(counts, c)
	}
	require.Len(t, counts, 1)
	require.Equal(t, 2, counts[0])

	unchangedInstanceID := "b072fa1e15fa4529001bb1ab81a7c2f2af63284811f4f9d6c2bc511f797218c8"
	updatedInstanceID := "32b68faa8644852c4ad79540b4bfeb1caf63284811f4f9d6c2bc511f797218c8"

	// Get the job type IDs for "com-tikal-jenkins-plugins-multijob-MultiJobProject" and "hudson-matrix-MatrixProject"
	multiJobID, err := pkg.GetJobTypeID(db, cache, "com-tikal-jenkins-plugins-multijob-MultiJobProject")
	require.NoError(t, err)
	matrixJobID, err := pkg.GetJobTypeID(db, cache, "hudson-matrix-MatrixProject")
	require.NoError(t, err)

	var firstReports []pkg.InstanceReport
	reportsQuery := pkg.PSQL(db).Select("id", "instance_id", "report_time", "year", "month", "version", "jvm_version_id",
		"executors", "count_for_month", "plugins", "jobs", "nodes").
		From(pkg.InstanceReportsTable).
		OrderBy("instance_id asc")

	rows, err := reportsQuery.Query()
	require.NoError(t, err)
	for rows.Next() {
		var ir pkg.InstanceReport
		require.NoError(t, rows.Scan(&ir.ID, &ir.InstanceID, &ir.ReportTime, &ir.Year, &ir.Month, &ir.Version, &ir.JVMVersionID, &ir.Executors, &ir.CountForMonth, &ir.Plugins, &ir.Jobs, &ir.Nodes))
		firstReports = append(firstReports, ir)
	}
	assert.Len(t, firstReports, 2)

	var unchangedFirstReport pkg.InstanceReport
	var updatedFirstReport pkg.InstanceReport
	for _, r := range firstReports {
		switch r.InstanceID {
		case unchangedInstanceID:
			unchangedFirstReport = r
		case updatedInstanceID:
			updatedFirstReport = r
		}
	}

	// There should be 11 MultiJobs in the initial report
	assert.Equal(t, 11, int(updatedFirstReport.Jobs[multiJobID]))
	// There should be 0 MatrixProjects in the initial report
	assert.Equal(t, 0, int(updatedFirstReport.Jobs[matrixJobID]))

	secondFile := filepath.Join("testdata", "day-later.json.gz")
	dayLaterReports, err := pkg.ParseDailyJSON(secondFile)
	require.NoError(t, err)

	for _, jr := range dayLaterReports {
		require.NoError(t, pkg.AddIndividualReport(db, cache, jr))
	}

	var secondReports []pkg.InstanceReport
	rows, err = reportsQuery.Query()
	require.NoError(t, err)
	for rows.Next() {
		var ir pkg.InstanceReport
		require.NoError(t, rows.Scan(&ir.ID, &ir.InstanceID, &ir.ReportTime, &ir.Year, &ir.Month, &ir.Version, &ir.JVMVersionID, &ir.Executors, &ir.CountForMonth, &ir.Plugins, &ir.Jobs, &ir.Nodes))
		secondReports = append(secondReports, ir)
	}

	// Make sure there are only two reports, since the second run should just overwrite the updatedInstanceID's report from the first run.
	assert.Len(t, secondReports, 2)

	var unchangedSecondReport pkg.InstanceReport
	var updatedSecondReport pkg.InstanceReport
	for _, r := range secondReports {
		switch r.InstanceID {
		case unchangedInstanceID:
			unchangedSecondReport = r
		case updatedInstanceID:
			updatedSecondReport = r
		}
	}

	assert.Equal(t, unchangedFirstReport, unchangedSecondReport)

	assert.NotEqual(t, updatedFirstReport, updatedSecondReport)
	// CountForMonth should be one higher
	assert.Equal(t, updatedFirstReport.CountForMonth+1, updatedSecondReport.CountForMonth)
	// There should be once less plugin in the second report
	assert.Len(t, updatedSecondReport.Plugins, len(updatedFirstReport.Plugins)-1)
	// There should be 0 MultiJobs
	assert.Equal(t, 0, int(updatedSecondReport.Jobs[multiJobID]))
	// There should be 10 MatrixProjects
	assert.Equal(t, 10, int(updatedSecondReport.Jobs[matrixJobID]))
}

// Fataler interface has a single method Fatal, which takes
// a slice of arguments and is expected to panic.
type Fataler interface {
	Fatal(args ...interface{})
}

// DBForTest connects to a local database for testing
func DBForTest(f Fataler) (sq.BaseRunner, func()) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://localhost/jenkins_usage_stats?sslmode=disable&timezone=UTC"
	}

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		f.Fatal(err)
	}

	closeFunc := func() {
		if err := db.Close(); err != nil {
			f.Fatal(err)
		}
	}

	if err := TruncateAll(db); err != nil {
		f.Fatal(err)
	}
	return sq.NewStmtCacheProxy(db), closeFunc
}

// TruncateAll takes a database connection, lists all the tables which
// aren't tracking schema_migrations and issues a cascading truncate
// across each of them.
func TruncateAll(db *sql.DB) error {
	rows, err := pkg.PSQL(db).
		Select("tablename").
		From("pg_tables").
		Where(sq.Eq{"schemaname": "public"}).
		Where(sq.NotEq{"tablename": "schema_migrations"}).
		Query()
	if err != nil {
		return err
	}

	var tables []string
	for rows.Next() {
		var tablename string
		if err := rows.Scan(&tablename); err != nil {
			return err
		}

		tables = append(tables, tablename)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	for _, table := range tables {
		truncate := fmt.Sprintf(`TRUNCATE TABLE %q CASCADE;`, table)
		if _, err := db.Exec(truncate); err != nil {
			return err
		}

		log.Println(truncate)
	}

	return nil
}
