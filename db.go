package stats

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/lib/pq"
)

const (
	// JVMVersionsTable is the jvm_versions table name
	JVMVersionsTable = "jvm_versions"
	// OSTypesTable is the os_types table name
	OSTypesTable = "os_types"
	// JobTypesTable is the job_types table name
	JobTypesTable = "job_types"
	// PluginsTable is the plugins table name
	PluginsTable = "plugins"
	// JenkinsVersionsTable is the jenkins_versions table name
	JenkinsVersionsTable = "jenkins_versions"
	// InstanceReportsTable is the instance_reports table name
	InstanceReportsTable = "instance_reports"

	questionVersion = "???"
)

// ReportFile records a daily report file which has been imported.
type ReportFile struct {
	Filename string `db:"filename"`
}

// JVMVersion represents a row in the jvm_versions table
type JVMVersion struct {
	ID   uint64 `db:"id"`
	Name string `db:"name"`
}

// OSType represents a row in the os_types table
type OSType struct {
	ID   uint64 `db:"id"`
	Name string `db:"name"`
}

// JobType represents a row in the job_types table
type JobType struct {
	ID   uint64 `db:"id"`
	Name string `db:"name"`
}

// Plugin represents a row in the plugins table
type Plugin struct {
	ID      uint64 `db:"id"`
	Name    string `db:"name"`
	Version string `db:"version"`
}

// JenkinsVersion represents a row in the jenkins_versions table
type JenkinsVersion struct {
	ID      uint64 `db:"id"`
	Version string `db:"version"`
}

// InstanceReport is a record of an individual instance's most recent report in a given month
type InstanceReport struct {
	ID            uint64          `db:"id"`
	InstanceID    string          `db:"instance_id"`
	ReportTime    time.Time       `db:"report_time"`
	Year          int             `db:"year"`
	Month         int             `db:"month"`
	Version       uint64          `db:"version"`
	JVMVersionID  uint64          `db:"jvm_version_id"`
	Executors     uint64          `db:"executors"`
	CountForMonth uint64          `db:"count_for_month"`
	Plugins       pq.Int64Array   `db:"plugins"`
	Jobs          *JobsForReport  `db:"jobs"`
	Nodes         *NodesForReport `db:"nodes"`
}

// PluginsForReport is a map of IDs from the "plugins" table seen on an instance report
type PluginsForReport []uint64

// Value is used for marshalling to JSON
func (p *PluginsForReport) Value() (driver.Value, error) {
	return json.Marshal(p)
}

// Scan is used for unmarshalling from JSON
func (p *PluginsForReport) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, &p)
}

// NodesForReport is a map of IDs from the "os_types" table to counts seen on an instance report
type NodesForReport map[uint64]uint64

// Value is used for marshalling to JSON
func (n *NodesForReport) Value() (driver.Value, error) {
	return json.Marshal(n)
}

// Scan is used for unmarshalling from JSON
func (n *NodesForReport) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, &n)
}

// JobsForReport is a map of IDs from the "job_types" table to counts seen on an instance report
type JobsForReport map[uint64]uint64

// Value is used for marshalling to JSON
func (j *JobsForReport) Value() (driver.Value, error) {
	return json.Marshal(j)
}

// Scan is used for unmarshalling from JSON
func (j *JobsForReport) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, &j)
}

// DBCache contains caching for the stats db
type DBCache struct {
	jvmVersions     map[string]uint64
	osTypes         map[string]uint64
	jobTypes        map[string]uint64
	jenkinsVersions map[string]uint64
	plugins         map[string]map[string]uint64

	getJVMVersionTime     time.Duration
	getOSTypeTime         time.Duration
	getJobTypeTime        time.Duration
	getJenkinsVersionTime time.Duration
	getPluginTime         time.Duration

	getInstanceReportTime    time.Duration
	insertInstanceReportTime time.Duration
	updateInstanceReportTime time.Duration
	insertNewReportsTime     time.Duration

	skippedForInstall int
	skippedForVersion int
	skippedForTime    int
	skippedForJobs    int
}

// ReportTimes returns a string with function times
func (sc *DBCache) ReportTimes() string {
	return fmt.Sprintf(`GetJVMVersion: %s
GetOSType: %s
GetJobType: %s
GetJenkinsVersion: %s
GetPlugin: %s
GetInstanceReport: %s
InsertInstanceReport: %s
UpdateInstanceReport: %s
InsertNewReports: %s
SkippedForInstall: %d
SkippedForVersion: %d
SkippedForTime: %d
SkippedForJobs: %d
`, sc.getJVMVersionTime.String(), sc.getOSTypeTime.String(), sc.getJobTypeTime.String(), sc.getJenkinsVersionTime.String(),
		sc.getPluginTime.String(), sc.getInstanceReportTime.String(), sc.insertInstanceReportTime.String(), sc.updateInstanceReportTime.String(), sc.insertNewReportsTime.String(),
		sc.skippedForInstall, sc.skippedForVersion, sc.skippedForTime, sc.skippedForJobs)
}

// NewStatsCache initializes a cache
func NewStatsCache() *DBCache {
	return &DBCache{
		jvmVersions:              map[string]uint64{},
		osTypes:                  map[string]uint64{},
		jobTypes:                 map[string]uint64{},
		jenkinsVersions:          map[string]uint64{},
		plugins:                  map[string]map[string]uint64{},
		getJVMVersionTime:        0,
		getOSTypeTime:            0,
		getJobTypeTime:           0,
		getJenkinsVersionTime:    0,
		getPluginTime:            0,
		getInstanceReportTime:    0,
		insertInstanceReportTime: 0,
		updateInstanceReportTime: 0,
		insertNewReportsTime:     0,
	}
}

// GetJVMVersionID gets the ID for the row of this version if it exists, and creates it and returns the ID if not
func GetJVMVersionID(db sq.BaseRunner, cache *DBCache, name string) (uint64, error) {
	start := time.Now()
	defer func() {
		cache.getJVMVersionTime += time.Since(start)
	}()
	if cached, ok := cache.jvmVersions[name]; ok {
		return cached, nil
	}
	var row JVMVersion
	err := PSQL(db).Select("id").From(JVMVersionsTable).
		Where(sq.Eq{"name": name}).
		QueryRow().
		Scan(&row.ID)
	if errors.Is(err, sql.ErrNoRows) {
		var id uint64
		q := PSQL(db).Insert(JVMVersionsTable).Columns("name").Values(name).Suffix(`RETURNING "id"`)
		err = q.QueryRow().Scan(&id)
		if err != nil {
			return 0, err
		}
		cache.jvmVersions[name] = id
		return id, nil
	}
	if err == nil {
		cache.jvmVersions[name] = row.ID
		return row.ID, nil
	}
	return 0, err
}

// GetOSTypeID gets the ID for the row of this OS if it exists, and creates it and returns the ID if not
func GetOSTypeID(db sq.BaseRunner, cache *DBCache, name string) (uint64, error) {
	start := time.Now()
	defer func() {
		cache.getOSTypeTime += time.Since(start)
	}()
	if name == "" {
		name = "N/A"
	}
	if cached, ok := cache.osTypes[name]; ok {
		return cached, nil
	}
	var row OSType
	err := PSQL(db).Select("id").From(OSTypesTable).
		Where(sq.Eq{"name": name}).
		QueryRow().
		Scan(&row.ID)
	if errors.Is(err, sql.ErrNoRows) {
		var id uint64
		q := PSQL(db).Insert(OSTypesTable).Columns("name").Values(name).Suffix(`RETURNING "id"`)
		err = q.QueryRow().Scan(&id)
		if err != nil {
			return 0, err
		}
		cache.osTypes[name] = id
		return id, nil
	}
	if err == nil {
		cache.osTypes[name] = row.ID
		return row.ID, nil
	}
	return 0, err
}

// GetJobTypeID gets the ID for the row of this job type if it exists, and creates it and returns the ID if not
func GetJobTypeID(db sq.BaseRunner, cache *DBCache, name string) (uint64, error) {
	start := time.Now()
	defer func() {
		cache.getJobTypeTime += time.Since(start)
	}()
	if cached, ok := cache.jobTypes[name]; ok {
		return cached, nil
	}
	var row JobType
	err := PSQL(db).Select("id").From(JobTypesTable).
		Where(sq.Eq{"name": name}).
		QueryRow().
		Scan(&row.ID)
	if errors.Is(err, sql.ErrNoRows) {
		var id uint64
		q := PSQL(db).Insert(JobTypesTable).Columns("name").Values(name).Suffix(`RETURNING "id"`)
		err = q.QueryRow().Scan(&id)
		if err != nil {
			return 0, err
		}
		cache.jobTypes[name] = id
		return id, nil
	}
	if err == nil {
		cache.jobTypes[name] = row.ID
		return row.ID, nil
	}
	return 0, err
}

// GetJenkinsVersionID gets the ID for the row of this version if it exists, and creates it and returns the ID if not
func GetJenkinsVersionID(db sq.BaseRunner, cache *DBCache, version string) (uint64, error) {
	start := time.Now()
	defer func() {
		cache.getJenkinsVersionTime += time.Since(start)
	}()
	if cached, ok := cache.jenkinsVersions[version]; ok {
		return cached, nil
	}
	var row JenkinsVersion
	err := PSQL(db).Select("id").From(JenkinsVersionsTable).
		Where(sq.Eq{"version": version}).
		QueryRow().
		Scan(&row.ID)
	if errors.Is(err, sql.ErrNoRows) {
		var id uint64
		q := PSQL(db).Insert(JenkinsVersionsTable).Columns("version").Values(version).Suffix(`RETURNING "id"`)
		err = q.QueryRow().Scan(&id)
		if err != nil {
			return 0, err
		}
		cache.jenkinsVersions[version] = id
		return id, nil
	}
	if err == nil {
		cache.jenkinsVersions[version] = row.ID
		return row.ID, nil
	}
	return 0, err
}

// GetPluginID gets the ID for the row of this plugin/version if it exists, and creates it and returns the ID if not
func GetPluginID(db sq.BaseRunner, cache *DBCache, name, version string) (uint64, error) {
	start := time.Now()
	defer func() {
		cache.getPluginTime += time.Since(start)
	}()
	if cachedPlugin, ok := cache.plugins[name]; ok {
		if cachedVersion, ok := cachedPlugin[version]; ok {
			return cachedVersion, nil
		}
	} else {
		cache.plugins[name] = make(map[string]uint64)
	}
	var row Plugin
	err := PSQL(db).Select("id").From(PluginsTable).
		Where(sq.Eq{"name": name}).
		Where(sq.Eq{"version": version}).
		QueryRow().
		Scan(&row.ID)
	if errors.Is(err, sql.ErrNoRows) {
		var id uint64
		q := PSQL(db).Insert(PluginsTable).Columns("name", "version").Values(name, version).Suffix(`RETURNING "id"`)
		err = q.QueryRow().Scan(&id)
		if err != nil {
			return 0, err
		}
		cache.plugins[name][version] = id
		return id, nil
	}
	if err == nil {
		cache.plugins[name][version] = row.ID
		return row.ID, nil
	}
	return 0, err
}

// AddIndividualReport adds/updates the JSON report to the database, along with all related tables.
func AddIndividualReport(db sq.BaseRunner, cache *DBCache, jsonReport *JSONReport) error {
	// Short-circuit for a few weird cases where the instance ID is >64 characters or the Jenkins version is >32 characters
	if len(jsonReport.Install) > 64 {
		cache.skippedForInstall++
		return nil
	}
	if len(jsonReport.Version) > 32 {
		cache.skippedForVersion++
		return nil
	}
	// Skip SNAPSHOT and weird ***/? Jenkins versions
	if strings.Contains(jsonReport.Version, "SNAPSHOT") || strings.Contains(jsonReport.Version, "***") || strings.Contains(jsonReport.Version, "?") {
		cache.skippedForVersion++
		return nil
	}

	ts, err := jsonReport.Timestamp()
	if err != nil {
		return err
	}

	insertRow := false

	// Check if there's an existing report.
	var report InstanceReport

	getReportStart := time.Now()
	var prevReport InstanceReport
	rows, err := PSQL(db).
		Select("id", "count_for_month, report_time").
		From(InstanceReportsTable).
		Where(sq.Eq{"instance_id": jsonReport.Install}).
		Where(sq.Eq{"year": ts.Year()}).
		Where(sq.Eq{"month": ts.Month()}).
		Query()
	defer func() {
		_ = rows.Close()
	}()
	if errors.Is(err, sql.ErrNoRows) {
		insertRow = true
	} else if err != nil {
		return err
	} else {
		for rows.Next() {
			err = rows.Scan(&prevReport.ID, &prevReport.CountForMonth, &prevReport.ReportTime)
			if err != nil {
				return err
			}
		}
	}
	cache.getInstanceReportTime += time.Since(getReportStart)

	if prevReport.CountForMonth == 0 {
		insertRow = true
	}

	report.CountForMonth = prevReport.CountForMonth + 1
	report.InstanceID = jsonReport.Install
	report.Year = ts.Year()
	report.Month = int(ts.Month())

	// If we already have a report for this install at this time, skip it.
	if prevReport.ReportTime == ts || ts.Before(prevReport.ReportTime) {
		cache.skippedForTime++

		if prevReport.CountForMonth == 1 {
			q := PSQL(db).Update(InstanceReportsTable).
				Where(sq.Eq{"id": prevReport.ID}).
				Set("count_for_month", report.CountForMonth)

			_, err = q.Exec()
			if err != nil {
				return err
			}

		}
		return nil
	}

	newReportsStart := time.Now()

	nodes := NodesForReport{}
	for _, jsonNode := range jsonReport.Nodes {
		if jsonNode.IsController {
			jvmVersionID, err := GetJVMVersionID(db, cache, jsonNode.JVMVersion)
			if err != nil {
				return err
			}
			report.JVMVersionID = jvmVersionID
		}
		// At least one report somehow screwed up and claims to have 32-bit max executors, so ignore that.
		if jsonNode.Executors != 2147483647 {
			report.Executors += jsonNode.Executors
		}

		osTypeID, err := GetOSTypeID(db, cache, jsonNode.OS)
		if err != nil {
			return err
		}
		if _, ok := nodes[osTypeID]; !ok {
			nodes[osTypeID] = 0
		}
		nodes[osTypeID]++
	}
	report.Nodes = &nodes

	if report.JVMVersionID == 0 {
		jvmVersionID, err := GetJVMVersionID(db, cache, "N/A")
		if err != nil {
			return err
		}
		report.JVMVersionID = jvmVersionID
	}

	var pluginIDs pq.Int64Array
	for _, jsonPlugin := range jsonReport.Plugins {
		// Exclude weird cases where there's no real version for the plugin
		if jsonPlugin.Version != questionVersion {
			pluginID, err := GetPluginID(db, cache, jsonPlugin.Name, jsonPlugin.Version)
			if err != nil {
				return err
			}
			pluginIDs = append(pluginIDs, int64(pluginID))
		}
	}
	report.Plugins = pluginIDs

	jobs := JobsForReport{}
	jobCount := uint64(0)
	for jobType, count := range jsonReport.Jobs {
		if count != 0 && !strings.HasPrefix(jobType, "private") {
			jobTypeID, err := GetJobTypeID(db, cache, jobType)
			if err != nil {
				return err
			}
			jobs[jobTypeID] = count
			jobCount += count
		}
	}
	if jobCount == 0 {
		cache.skippedForJobs++
		return nil
	}
	report.Jobs = &jobs
	cache.insertNewReportsTime += time.Since(newReportsStart)

	report.ReportTime = ts

	jvID, err := GetJenkinsVersionID(db, cache, jsonReport.Version)
	if err != nil {
		return err
	}
	report.Version = jvID

	if insertRow {
		insertStart := time.Now()
		_, err = PSQL(db).Insert(InstanceReportsTable).
			Columns("instance_id", "report_time", "year", "month", "version", "jvm_version_id",
				"executors", "count_for_month", "plugins", "jobs", "nodes").
			Values(report.InstanceID,
				report.ReportTime,
				report.Year,
				report.Month,
				report.Version,
				report.JVMVersionID,
				report.Executors,
				report.CountForMonth,
				report.Plugins,
				report.Jobs,
				report.Nodes).
			Exec()
		if err != nil {
			return err
		}
		cache.insertInstanceReportTime += time.Since(insertStart)
	} else {
		updateStart := time.Now()
		q := PSQL(db).Update(InstanceReportsTable).
			Where(sq.Eq{"id": prevReport.ID}).
			Set("count_for_month", report.CountForMonth).
			Set("report_time", report.ReportTime).
			Set("version", report.Version).
			Set("jvm_version_id", report.JVMVersionID).
			Set("executors", report.Executors).
			Set("plugins", report.Plugins).
			Set("jobs", report.Jobs).
			Set("nodes", report.Nodes)

		_, err = q.Exec()
		cache.updateInstanceReportTime += time.Since(updateStart)
		if err != nil {
			return err
		}
	}

	return nil
}

// ReportAlreadyRead checks if a filename has already been read and processed
func ReportAlreadyRead(db sq.BaseRunner, filename string) (bool, error) {
	rows, err := PSQL(db).Select("count(*)").
		From("report_files").
		Where(sq.Eq{"filename": filename}).
		Query()
	defer func() {
		_ = rows.Close()
	}()
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}

	for rows.Next() {
		var c int
		err := rows.Scan(&c)
		if err != nil {
			return false, err
		}
		if c > 0 {
			return true, nil
		}
	}
	return false, nil
}

// MarkReportRead records that we've read and processed a filename.
func MarkReportRead(db sq.BaseRunner, filename string) error {
	_, err := PSQL(db).Insert("report_files").Columns("filename").Values(filename).Exec()
	return err
}

// PSQL is a postgresql squirrel statement builder
func PSQL(db sq.BaseRunner) sq.StatementBuilderType {
	return sq.StatementBuilder.PlaceholderFormat(sq.Dollar).RunWith(db)
}

func startDateForYearMonth(year int, month int) time.Time {
	return time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.FixedZone("", 0))
}
