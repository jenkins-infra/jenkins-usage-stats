package pkg

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
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
	ID               uint64           `db:"id"`
	InstanceID       string           `db:"instance_id"`
	ReportTime       time.Time        `db:"report_time"`
	Year             uint16           `db:"year"`
	Month            uint16           `db:"month"`
	Version          uint64           `db:"version"`
	ServletContainer sql.NullString   `db:"servlet_container"`
	JVMVersionID     uint64           `db:"jvm_version_id"`
	Executors        uint64           `db:"executors"`
	JVMName          sql.NullString   `db:"jvm_name"`
	JVMVendor        sql.NullString   `db:"jvm_vendor"`
	CountForMonth    uint64           `db:"count_for_month"`
	Plugins          PluginsForReport `db:"plugins"`
	Jobs             JobsForReport    `db:"jobs"`
	Nodes            NodesForReport   `db:"nodes"`
}

// PluginsForReport is a map of IDs from the "plugins" table seen on an instance report
type PluginsForReport []uint64

// Value is used for marshalling to JSON
func (p PluginsForReport) Value() (driver.Value, error) {
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
func (n NodesForReport) Value() (driver.Value, error) {
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
func (j JobsForReport) Value() (driver.Value, error) {
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

// StatsCache contains caching for the stats db
type StatsCache struct {
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
}

// ReportTimes returns a string with function times
func (sc *StatsCache) ReportTimes() string {
	return fmt.Sprintf(`GetJVMVersion: %s
GetOSType: %s
GetJobType: %s
GetJenkinsVersion: %s
GetPlugin: %s
GetInstanceReport: %s
InsertInstanceReport: %s
UpdateInstanceReport: %s
InsertNewReports: %s
`, sc.getJVMVersionTime.String(), sc.getOSTypeTime.String(), sc.getJobTypeTime.String(), sc.getJenkinsVersionTime.String(),
		sc.getPluginTime.String(), sc.getInstanceReportTime.String(), sc.insertInstanceReportTime.String(), sc.updateInstanceReportTime.String(), sc.insertNewReportsTime.String())
}

// NewStatsCache initializes a cache
func NewStatsCache() *StatsCache {
	return &StatsCache{
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
func GetJVMVersionID(db sq.BaseRunner, cache *StatsCache, name string) (uint64, error) {
	start := time.Now()
	defer func() {
		cache.getJVMVersionTime += time.Since(start)
	}()
	if cached, ok := cache.jvmVersions[name]; ok {
		return cached, nil
	}
	var row JVMVersion
	err := PSQL().RunWith(db).Select("id").From("jvm_versions").
		Where(sq.Eq{"name": name}).
		QueryRow().
		Scan(&row.ID)
	if err == sql.ErrNoRows {
		var id uint64
		q := PSQL().RunWith(db).Insert("jvm_versions").Columns("name").Values(name).Suffix(`RETURNING "id"`)
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
func GetOSTypeID(db sq.BaseRunner, cache *StatsCache, name string) (uint64, error) {
	start := time.Now()
	defer func() {
		cache.getOSTypeTime += time.Since(start)
	}()
	if cached, ok := cache.osTypes[name]; ok {
		return cached, nil
	}
	var row OSType
	err := PSQL().RunWith(db).Select("id").From("os_types").
		Where(sq.Eq{"name": name}).
		QueryRow().
		Scan(&row.ID)
	if err == sql.ErrNoRows {
		var id uint64
		q := PSQL().RunWith(db).Insert("os_types").Columns("name").Values(name).Suffix(`RETURNING "id"`)
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
func GetJobTypeID(db sq.BaseRunner, cache *StatsCache, name string) (uint64, error) {
	start := time.Now()
	defer func() {
		cache.getJobTypeTime += time.Since(start)
	}()
	if cached, ok := cache.jobTypes[name]; ok {
		return cached, nil
	}
	var row JobType
	err := PSQL().RunWith(db).Select("id").From("job_types").
		Where(sq.Eq{"name": name}).
		QueryRow().
		Scan(&row.ID)
	if err == sql.ErrNoRows {
		var id uint64
		q := PSQL().RunWith(db).Insert("job_types").Columns("name").Values(name).Suffix(`RETURNING "id"`)
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
func GetJenkinsVersionID(db sq.BaseRunner, cache *StatsCache, version string) (uint64, error) {
	start := time.Now()
	defer func() {
		cache.getJenkinsVersionTime += time.Since(start)
	}()
	if cached, ok := cache.jenkinsVersions[version]; ok {
		return cached, nil
	}
	var row JenkinsVersion
	err := PSQL().RunWith(db).Select("id").From("jenkins_versions").
		Where(sq.Eq{"version": version}).
		QueryRow().
		Scan(&row.ID)
	if err == sql.ErrNoRows {
		var id uint64
		q := PSQL().RunWith(db).Insert("jenkins_versions").Columns("version").Values(version).Suffix(`RETURNING "id"`)
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
func GetPluginID(db sq.BaseRunner, cache *StatsCache, name, version string) (uint64, error) {
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
	err := PSQL().RunWith(db).Select("id").From("plugins").
		Where(sq.Eq{"name": name}).
		Where(sq.Eq{"version": version}).
		QueryRow().
		Scan(&row.ID)
	if err == sql.ErrNoRows {
		var id uint64
		q := PSQL().RunWith(db).Insert("plugins").Columns("name", "version").Values(name, version).Suffix(`RETURNING "id"`)
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

// AddReport adds/updates the JSON report to the database, along with all related tables.
func AddReport(db sq.BaseRunner, cache *StatsCache, jsonReport *JSONReport) error {
	ts, err := jsonReport.Timestamp()
	if err != nil {
		return err
	}

	insertRow := false

	// Check if there's an existing report.
	var report InstanceReport
	getReportStart := time.Now()
	rows, err := PSQL().RunWith(db).
		Select("id", "count_for_month, report_time").
		From("instance_reports").
		Where(sq.Eq{"instance_id": jsonReport.Install}).
		Where(sq.Eq{"year": ts.Year()}).
		Where(sq.Eq{"month": ts.Month()}).
		Query()
	defer func() {
		_ = rows.Close()
	}()
	cache.getInstanceReportTime += time.Since(getReportStart)
	if err == sql.ErrNoRows {
		insertRow = true
	} else if err != nil {
		return err
	} else {
		for rows.Next() {
			var c uint64
			var rt time.Time
			var id uint64
			err = rows.Scan(&id, &c, &rt)
			if err != nil {
				return err
			}
			report.ID = id
			report.CountForMonth = c
			report.ReportTime = rt
		}
	}

	if report.CountForMonth == 0 {
		insertRow = true
	}

	report.CountForMonth++
	report.InstanceID = jsonReport.Install
	report.Year = uint16(ts.Year())
	report.Month = uint16(ts.Month())

	// If we already have a report for this install at this time, skip it.
	if report.ReportTime == ts {
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

			if jsonNode.JVMName != "" {
				report.JVMName = sql.NullString{String: jsonNode.JVMName, Valid: true}
			}
			if jsonNode.JVMVendor != "" {
				report.JVMVendor = sql.NullString{String: jsonNode.JVMVendor, Valid: true}
			}
		}
		report.Executors += jsonNode.Executors

		osTypeID, err := GetOSTypeID(db, cache, jsonNode.OS)
		if err != nil {
			return err
		}
		if _, ok := nodes[osTypeID]; !ok {
			nodes[osTypeID] = 0
		}
		nodes[osTypeID]++
	}
	report.Nodes = nodes

	// If we don't have any plugins or a controller, skip.
	if len(jsonReport.Plugins) == 0 || report.JVMVersionID == 0 {
		return nil
	}

	var pluginIDs PluginsForReport
	for _, jsonPlugin := range jsonReport.Plugins {
		pluginID, err := GetPluginID(db, cache, jsonPlugin.Name, jsonPlugin.Version)
		if err != nil {
			return err
		}
		pluginIDs = append(pluginIDs, pluginID)
	}
	report.Plugins = pluginIDs

	jobs := JobsForReport{}
	for jobType, count := range jsonReport.Jobs {
		if count != 0 {
			jobTypeID, err := GetJobTypeID(db, cache, jobType)
			if err != nil {
				return err
			}
			jobs[jobTypeID] = count
		}
	}
	report.Jobs = jobs
	cache.insertNewReportsTime += time.Since(newReportsStart)

	report.ReportTime = ts
	if jsonReport.ServletContainer != "" {
		report.ServletContainer = sql.NullString{String: jsonReport.ServletContainer}
	}

	jvID, err := GetJenkinsVersionID(db, cache, jsonReport.Version)
	if err != nil {
		return err
	}
	report.Version = jvID

	if err != nil {
		return err
	}

	//	alreadySeenPlugins := make(map[uint64]uint64)
	if insertRow {
		insertStart := time.Now()
		_, err = PSQL().RunWith(db).Insert("instance_reports").
			Columns("instance_id", "report_time", "year", "month", "version", "servlet_container", "jvm_version_id",
				"executors", "jvm_name", "jvm_vendor", "count_for_month", "plugins", "jobs", "nodes").
			Values(report.InstanceID,
				report.ReportTime,
				report.Year,
				report.Month,
				report.Version,
				report.ServletContainer,
				report.JVMVersionID,
				report.Executors,
				report.JVMName,
				report.JVMVendor,
				1,
				report.Plugins,
				report.Jobs,
				report.Nodes).
			Exec()
		cache.insertInstanceReportTime += time.Since(insertStart)
		if err == nil {
			return err
		}
	} else {
		updateStart := time.Now()
		_, err = PSQL().RunWith(db).Update("instance_reports").Where(sq.Eq{"id": report.ID}).
			Set("report_time", report.ReportTime).
			Set("version", report.Version).
			Set("servlet_container", report.ServletContainer).
			Set("count_for_month", report.CountForMonth).
			Set("jvm_version_id", report.JVMVersionID).
			Set("jvm_vendor", report.JVMVendor).
			Set("jvm_name", report.JVMName).
			Set("executors", report.Executors).
			Set("plugins", report.Plugins).
			Set("jobs", report.Jobs).
			Set("nodes", report.Nodes).
			Exec()
		cache.updateInstanceReportTime += time.Since(updateStart)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetInstallCountForVersions generates a map of Jenkins versions to install counts
func GetInstallCountForVersions(db sq.BaseRunner, year, month string) (map[string]uint64, error) {
	rows, err := PSQL().Select("jenkins_versions.version as version", "count(*) as number").
		From("instance_reports").
		Join("jenkins_versions on instance_reports.version = jenkins_versions.id").
		Where(sq.Eq{"instance_reports.year": year}).
		Where(sq.Eq{"instance_reports.month": month}).
		Where(sq.GtOrEq{"instance_reports.count_for_month": 2}).
		GroupBy("version").
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	versionMap := make(map[string]uint64)
	for rows.Next() {
		var v string
		var c uint64
		err := rows.Scan(&v, &c)
		if err != nil {
			return nil, err
		}
		versionMap[v] = c
	}

	return versionMap, nil
}

// GetPluginCounts generates a map of plugin name and install counts
func GetPluginCounts(db sq.BaseRunner, year, month string) (map[string]uint64, error) {
	rows, err := PSQL().Select("p.name as pn", "count(*) as number").
		From("instance_reports i").
		From("jsonb_array_elements_text(i.plugins) pr(id)").
		Join("plugins p on p.id::text = pr.id").
		Where(sq.Eq{"i.year": year}).
		Where(sq.Eq{"i.month": month}).
		Where(sq.GtOrEq{"i.count_for_month": 2}).
		GroupBy("name").
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	pluginMap := make(map[string]uint64)
	for rows.Next() {
		var p string
		var c uint64
		err := rows.Scan(&p, &c)
		if err != nil {
			return nil, err
		}
		pluginMap[p] = c
	}

	return pluginMap, nil
}

// GetCapabilities generates a map of Jenkins versions and install counts for that version and all earlier ones
func GetCapabilities(db sq.BaseRunner, year, month string) (map[string]uint64, error) {
	rows, err := PSQL().Select("jenkins_versions.version as version", "count(*) as number").
		From("instance_reports").
		Join("jenkins_versions on instance_reports.version = jenkins_versions.id").
		Where(sq.Eq{"instance_reports.year": year}).
		Where(sq.Eq{"instance_reports.month": month}).
		Where(sq.GtOrEq{"instance_reports.count_for_month": 2}).
		GroupBy("version").
		OrderBy("version DESC").
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	higherCapabilityCount := uint64(0)
	versionMap := make(map[string]uint64)
	for rows.Next() {
		var p string
		var c uint64
		err := rows.Scan(&p, &c)
		if err != nil {
			return nil, err
		}
		versionMap[p] = c + higherCapabilityCount
	}

	return versionMap, nil
}

// ReportAlreadyRead checks if a filename has already been read and processed
func ReportAlreadyRead(db sq.BaseRunner, filename string) (bool, error) {
	rows, err := PSQL().Select("count(*)").
		From("report_files").
		Where(sq.Eq{"filename": filename}).
		RunWith(db).
		Query()
	defer func() {
		_ = rows.Close()
	}()
	if err != nil {
		if err == sql.ErrNoRows {
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
	_, err := PSQL().RunWith(db).Insert("report_files").Columns("filename").Values(filename).Exec()
	return err
}

// PSQL is a postgresql squirrel statement builder
func PSQL() sq.StatementBuilderType {
	return sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
}
