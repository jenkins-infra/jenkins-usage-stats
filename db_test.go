package stats_test

import (
	"database/sql"
	"path/filepath"
	"testing"

	sq "github.com/Masterminds/squirrel"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	stats "github.com/jenkins-infra/jenkins-usage-stats"
	"github.com/jenkins-infra/jenkins-usage-stats/testutil"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetJVMVersionID(t *testing.T) {
	db, closeFunc := testutil.DBForTest(t)
	defer closeFunc()

	cache := stats.NewStatsCache()
	firstVer := "1.7"
	secondVer := "13"

	var fetchedVersion stats.JVMVersion
	err := stats.PSQL(db).Select("id", "name").From(stats.JVMVersionsTable).Where(sq.Eq{"name": firstVer}).
		QueryRow().Scan(&fetchedVersion.ID, &fetchedVersion.Name)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := stats.GetJVMVersionID(db, cache, firstVer)
	require.NoError(t, err)
	require.NoError(t, stats.PSQL(db).Select("id", "name").From(stats.JVMVersionsTable).Where(sq.Eq{"name": firstVer}).
		QueryRow().Scan(&fetchedVersion.ID, &fetchedVersion.Name))
	assert.Equal(t, firstID, fetchedVersion.ID)

	secondID, err := stats.GetJVMVersionID(db, cache, secondVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, secondID)
}

func TestGetOSTypeID(t *testing.T) {
	db, closeFunc := testutil.DBForTest(t)
	defer closeFunc()

	cache := stats.NewStatsCache()

	firstVer := "Windows 11"
	secondVer := "Ubuntu something"

	var fetchedOS stats.OSType
	err := stats.PSQL(db).Select("id", "name").From(stats.OSTypesTable).Where(sq.Eq{"name": firstVer}).
		QueryRow().Scan(&fetchedOS.ID, &fetchedOS.Name)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := stats.GetOSTypeID(db, cache, firstVer)
	require.NoError(t, err)
	require.NoError(t, stats.PSQL(db).Select("id", "name").From(stats.OSTypesTable).Where(sq.Eq{"name": firstVer}).
		QueryRow().Scan(&fetchedOS.ID, &fetchedOS.Name))
	assert.Equal(t, firstID, fetchedOS.ID)

	secondID, err := stats.GetOSTypeID(db, cache, secondVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, secondID)
}

func TestGetJobTypeID(t *testing.T) {
	db, closeFunc := testutil.DBForTest(t)
	defer closeFunc()

	cache := stats.NewStatsCache()

	firstVer := "hudson-maven-MavenModuleSet"
	secondVer := "org-jenkinsci-plugins-workflow-job-WorkflowJob"

	var fetchedJobType stats.JobType
	err := stats.PSQL(db).Select("id", "name").From(stats.JobTypesTable).Where(sq.Eq{"name": firstVer}).
		QueryRow().Scan(&fetchedJobType.ID, &fetchedJobType.Name)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := stats.GetJobTypeID(db, cache, firstVer)
	require.NoError(t, err)
	require.NoError(t, stats.PSQL(db).Select("id", "name").From(stats.JobTypesTable).Where(sq.Eq{"name": firstVer}).
		QueryRow().Scan(&fetchedJobType.ID, &fetchedJobType.Name))
	assert.Equal(t, firstID, fetchedJobType.ID)

	secondID, err := stats.GetJobTypeID(db, cache, secondVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, secondID)
}

func TestGetJenkinsVersionID(t *testing.T) {
	db, closeFunc := testutil.DBForTest(t)
	defer closeFunc()

	cache := stats.NewStatsCache()

	firstVer := "1.500"
	secondVer := "2.201.1"

	var fetchedJV stats.JenkinsVersion
	err := stats.PSQL(db).Select("id", "version").From(stats.JenkinsVersionsTable).Where(sq.Eq{"version": firstVer}).
		QueryRow().Scan(&fetchedJV.ID, &fetchedJV.Version)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := stats.GetJenkinsVersionID(db, cache, firstVer)
	require.NoError(t, err)
	require.NoError(t, stats.PSQL(db).Select("id", "version").From(stats.JenkinsVersionsTable).Where(sq.Eq{"version": firstVer}).
		QueryRow().Scan(&fetchedJV.ID, &fetchedJV.Version))
	assert.Equal(t, firstID, fetchedJV.ID)

	secondID, err := stats.GetJenkinsVersionID(db, cache, secondVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, secondID)
}

func TestGetPluginID(t *testing.T) {
	db, closeFunc := testutil.DBForTest(t)
	defer closeFunc()

	cache := stats.NewStatsCache()

	firstName := "first-plugin"
	firstVer := "1.0"
	secondVer := "2.0"
	secondName := "second-plugin"

	var fetchedPlugin stats.Plugin
	err := stats.PSQL(db).Select("id", "name", "version").From(stats.PluginsTable).Where(sq.Eq{"name": firstName}).Where(sq.Eq{"version": firstVer}).
		QueryRow().Scan(&fetchedPlugin.ID, &fetchedPlugin.Name, &fetchedPlugin.Version)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := stats.GetPluginID(db, cache, firstName, firstVer)
	require.NoError(t, err)
	require.NoError(t, stats.PSQL(db).Select("id", "name", "version").From(stats.PluginsTable).Where(sq.Eq{"name": firstName}).Where(sq.Eq{"version": firstVer}).
		QueryRow().Scan(&fetchedPlugin.ID, &fetchedPlugin.Name, &fetchedPlugin.Version))
	assert.Equal(t, firstID, fetchedPlugin.ID)

	secondID, err := stats.GetPluginID(db, cache, firstName, secondVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, secondID)

	otherPluginID, err := stats.GetPluginID(db, cache, secondName, firstVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, otherPluginID)
}

func TestAddIndividualReport(t *testing.T) {
	db, closeFunc := testutil.DBForTest(t)
	defer closeFunc()

	cache := stats.NewStatsCache()

	initialFile := filepath.Join("testdata", "base.json.gz")
	jsonReports, err := stats.ParseDailyJSON(initialFile)
	require.NoError(t, err)

	for _, jr := range jsonReports {
		require.NoError(t, stats.AddIndividualReport(db, cache, jr))
	}

	result, err := stats.PSQL(db).Select("count(*)").
		From(stats.InstanceReportsTable).
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
	multiJobID, err := stats.GetJobTypeID(db, cache, "com-tikal-jenkins-plugins-multijob-MultiJobProject")
	require.NoError(t, err)
	matrixJobID, err := stats.GetJobTypeID(db, cache, "hudson-matrix-MatrixProject")
	require.NoError(t, err)

	var firstReports []stats.InstanceReport
	reportsQuery := stats.PSQL(db).Select("id", "instance_id", "report_time", "year", "month", "version", "jvm_version_id",
		"executors", "count_for_month", "plugins", "jobs", "nodes").
		From(stats.InstanceReportsTable).
		OrderBy("instance_id asc")

	rows, err := reportsQuery.Query()
	require.NoError(t, err)
	for rows.Next() {
		var ir stats.InstanceReport
		require.NoError(t, rows.Scan(&ir.ID, &ir.InstanceID, &ir.ReportTime, &ir.Year, &ir.Month, &ir.Version, &ir.JVMVersionID, &ir.Executors, &ir.CountForMonth, &ir.Plugins, &ir.Jobs, &ir.Nodes))
		firstReports = append(firstReports, ir)
	}
	assert.Len(t, firstReports, 2)

	var unchangedFirstReport stats.InstanceReport
	var updatedFirstReport stats.InstanceReport
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
	dayLaterReports, err := stats.ParseDailyJSON(secondFile)
	require.NoError(t, err)

	for _, jr := range dayLaterReports {
		require.NoError(t, stats.AddIndividualReport(db, cache, jr))
	}

	var secondReports []stats.InstanceReport
	rows, err = reportsQuery.Query()
	require.NoError(t, err)
	for rows.Next() {
		var ir stats.InstanceReport
		require.NoError(t, rows.Scan(&ir.ID, &ir.InstanceID, &ir.ReportTime, &ir.Year, &ir.Month, &ir.Version, &ir.JVMVersionID, &ir.Executors, &ir.CountForMonth, &ir.Plugins, &ir.Jobs, &ir.Nodes))
		secondReports = append(secondReports, ir)
	}

	// Make sure there are only two reports, since the second run should just overwrite the updatedInstanceID's report from the first run.
	assert.Len(t, secondReports, 2)

	var unchangedSecondReport stats.InstanceReport
	var updatedSecondReport stats.InstanceReport
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
