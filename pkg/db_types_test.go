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
	"github.com/jmoiron/sqlx"
)

func TestGetJVMVersionID(t *testing.T) {
	db, closeFunc := DBForTest(t)
	defer closeFunc()

	firstVer := "1.7"
	secondVer := "13"

	var fetchedVersion pkg.JVMVersion
	err := db.Get(&fetchedVersion, "SELECT * FROM jvm_versions WHERE name = $1", firstVer)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := pkg.GetJVMVersionID(db, firstVer)
	require.NoError(t, err)
	require.NoError(t, db.Get(&fetchedVersion, "SELECT * FROM jvm_versions WHERE name = $1", firstVer))
	assert.Equal(t, firstID, fetchedVersion.ID)

	secondID, err := pkg.GetJVMVersionID(db, secondVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, secondID)
}

func TestGetOSTypeID(t *testing.T) {
	db, closeFunc := DBForTest(t)
	defer closeFunc()

	firstVer := "Windows 11"
	secondVer := "Ubuntu something"

	var fetchedOS pkg.OSType
	err := db.Get(&fetchedOS, "SELECT * FROM os_types WHERE name = $1", firstVer)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := pkg.GetOSTypeID(db, firstVer)
	require.NoError(t, err)
	require.NoError(t, db.Get(&fetchedOS, "SELECT * FROM os_types WHERE name = $1", firstVer))
	assert.Equal(t, firstID, fetchedOS.ID)

	secondID, err := pkg.GetOSTypeID(db, secondVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, secondID)
}

func TestGetJobTypeID(t *testing.T) {
	db, closeFunc := DBForTest(t)
	defer closeFunc()

	firstVer := "hudson-maven-MavenModuleSet"
	secondVer := "org-jenkinsci-plugins-workflow-job-WorkflowJob"

	var fetchedJobType pkg.JobType
	err := db.Get(&fetchedJobType, "SELECT * FROM job_types WHERE name = $1", firstVer)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := pkg.GetJobTypeID(db, firstVer)
	require.NoError(t, err)
	require.NoError(t, db.Get(&fetchedJobType, "SELECT * FROM job_types WHERE name = $1", firstVer))
	assert.Equal(t, firstID, fetchedJobType.ID)

	secondID, err := pkg.GetJobTypeID(db, secondVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, secondID)
}

func TestGetJenkinsVersionID(t *testing.T) {
	db, closeFunc := DBForTest(t)
	defer closeFunc()

	firstVer := "1.500"
	secondVer := "2.201.1"

	var fetchedJV pkg.JenkinsVersion
	err := db.Get(&fetchedJV, "SELECT * FROM jenkins_versions WHERE version = $1", firstVer)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := pkg.GetJenkinsVersionID(db, firstVer)
	require.NoError(t, err)
	require.NoError(t, db.Get(&fetchedJV, "SELECT * FROM jenkins_versions WHERE version = $1", firstVer))
	assert.Equal(t, firstID, fetchedJV.ID)

	secondID, err := pkg.GetJenkinsVersionID(db, secondVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, secondID)
}

func TestGetPluginID(t *testing.T) {
	db, closeFunc := DBForTest(t)
	defer closeFunc()

	firstName := "first-plugin"
	firstVer := "1.0"
	secondVer := "2.0"
	secondName := "second-plugin"

	var fetchedPlugin pkg.Plugin
	err := db.Get(&fetchedPlugin, "SELECT * FROM plugins WHERE name = $1 and version = $2", firstName, firstVer)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := pkg.GetPluginID(db, firstName, firstVer)
	require.NoError(t, err)
	require.NoError(t, db.Get(&fetchedPlugin, "SELECT * FROM plugins WHERE name = $1 and version = $2", firstName, firstVer))
	assert.Equal(t, firstID, fetchedPlugin.ID)

	secondID, err := pkg.GetPluginID(db, firstName, secondVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, secondID)

	otherPluginID, err := pkg.GetPluginID(db, secondName, firstVer)
	require.NoError(t, err)
	assert.NotEqual(t, firstID, otherPluginID)
}

func TestAddReport(t *testing.T) {
	db, closeFunc := DBForTest(t)
	defer closeFunc()

	initialFile := filepath.Join("testdata", "base.json.gz")
	jsonReports, err := pkg.ParseDailyJSON(initialFile)
	require.NoError(t, err)

	for _, jr := range jsonReports {
		require.NoError(t, pkg.AddReport(db, jr))
	}

	result, err := pkg.PSQL().Select("count(*)").
		From("instance_reports").
		RunWith(db).
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
	multiJobID, err := pkg.GetJobTypeID(db, "com-tikal-jenkins-plugins-multijob-MultiJobProject")
	require.NoError(t, err)
	matrixJobID, err := pkg.GetJobTypeID(db, "hudson-matrix-MatrixProject")
	require.NoError(t, err)

	var firstReports []pkg.InstanceReport
	require.NoError(t, db.Select(&firstReports, "SELECT * FROM instance_reports order by instance_id asc"))

	firstJobsForReports, firstNodesForReports, firstPluginsForReports := getSubReports(t, db, firstReports)

	// There should be 11 MultiJobs in the initial report
	assert.Equal(t, 11, int(firstJobsForReports[updatedInstanceID][multiJobID]))
	// There should be 0 MatrixProjects in the initial report
	assert.Equal(t, 0, int(firstJobsForReports[updatedInstanceID][matrixJobID]))

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

	secondFile := filepath.Join("testdata", "day-later.json.gz")
	dayLaterReports, err := pkg.ParseDailyJSON(secondFile)
	require.NoError(t, err)

	for _, jr := range dayLaterReports {
		require.NoError(t, pkg.AddReport(db, jr))
	}

	var secondReports []pkg.InstanceReport
	require.NoError(t, db.Select(&secondReports, "SELECT * FROM instance_reports order by instance_id asc"))

	secondJobsForReports, secondNodesForReports, secondPluginsForReports := getSubReports(t, db, secondReports)

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
	assert.Equal(t, firstJobsForReports[unchangedInstanceID], secondJobsForReports[unchangedInstanceID])
	assert.Equal(t, firstNodesForReports[unchangedInstanceID], secondNodesForReports[unchangedInstanceID])
	assert.Equal(t, firstPluginsForReports[unchangedInstanceID], secondPluginsForReports[unchangedInstanceID])

	assert.NotEqual(t, updatedFirstReport, updatedSecondReport)
	assert.NotEqual(t, firstJobsForReports[updatedInstanceID], secondJobsForReports[updatedInstanceID])
	assert.NotEqual(t, firstNodesForReports[updatedInstanceID], secondNodesForReports[updatedInstanceID])
	assert.NotEqual(t, firstPluginsForReports[updatedInstanceID], secondPluginsForReports[updatedInstanceID])
	// CountForMonth should be one higher
	assert.Equal(t, updatedFirstReport.CountForMonth+1, updatedSecondReport.CountForMonth)
	// There should be once less plugin in the second report
	assert.Len(t, secondPluginsForReports[updatedInstanceID], len(firstPluginsForReports[updatedInstanceID])-1)
	// There should be 0 MultiJobs
	assert.Equal(t, 0, int(secondJobsForReports[updatedInstanceID][multiJobID]))
	// There should be 10 MatrixProjects
	assert.Equal(t, 10, int(secondJobsForReports[updatedInstanceID][matrixJobID]))
}

func getSubReports(t *testing.T, db *sqlx.DB, instanceReports []pkg.InstanceReport) (map[string]map[uint64]uint64, map[string][]pkg.NodeReport, map[string][]pkg.PluginReport) {
	jobReports := make(map[string]map[uint64]uint64)
	nodeReports := make(map[string][]pkg.NodeReport)
	pluginReports := make(map[string][]pkg.PluginReport)

	for _, r := range instanceReports {
		jobReports[r.InstanceID] = make(map[uint64]uint64)
		var jobReport pkg.JobReport
		jobRows, err := db.Queryx("SELECT * FROM job_reports where report_id = $1", r.ID)
		require.NoError(t, err)
		for jobRows.Next() {
			require.NoError(t, jobRows.StructScan(&jobReport))
			jobReports[r.InstanceID][jobReport.JobTypeID] = jobReport.Count
		}

		nodeReports[r.InstanceID] = []pkg.NodeReport{}
		var nodeReport pkg.NodeReport
		nodeRows, err := db.Queryx("SELECT * FROM nodes where report_id = $1", r.ID)
		require.NoError(t, err)
		for nodeRows.Next() {
			require.NoError(t, nodeRows.StructScan(&nodeReport))
			nodeReports[r.InstanceID] = append(nodeReports[r.InstanceID], nodeReport)
		}

		pluginReports[r.InstanceID] = []pkg.PluginReport{}
		var pluginReport pkg.PluginReport
		pluginRows, err := db.Queryx("SELECT * FROM plugin_reports where report_id = $1", r.ID)
		require.NoError(t, err)
		for pluginRows.Next() {
			require.NoError(t, pluginRows.StructScan(&pluginReport))
			pluginReports[r.InstanceID] = append(pluginReports[r.InstanceID], pluginReport)
		}
	}

	return jobReports, nodeReports, pluginReports
}

// Fataler interface has a single method Fatal, which takes
// a slice of arguments and is expected to panic.
type Fataler interface {
	Fatal(args ...interface{})
}

// DBForTest connects to a local database for testing
func DBForTest(f Fataler) (*sqlx.DB, func()) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://localhost/jenkins_usage_stats?sslmode=disable&timezone=UTC"
	}

	db, err := sqlx.Open("postgres", databaseURL)
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
	return db, closeFunc
}

// TruncateAll takes a database connection, lists all the tables which
// aren't tracking schema_migrations and issues a cascading truncate
// across each of them.
func TruncateAll(db *sqlx.DB) error {
	rows, err := pkg.PSQL().
		Select("tablename").
		From("pg_tables").
		Where(sq.Eq{"schemaname": "public"}).
		Where(sq.NotEq{"tablename": "schema_migrations"}).
		RunWith(db).
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
