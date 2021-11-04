package pkg

import (
	"database/sql"
	"time"

	sq "github.com/Masterminds/squirrel"

	"github.com/hashicorp/go-multierror"

	"github.com/jmoiron/sqlx"
)

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
	ID               uint64         `db:"id"`
	InstanceID       string         `db:"instance_id"`
	ReportTime       time.Time      `db:"report_time"`
	Year             uint16         `db:"year"`
	Month            uint16         `db:"month"`
	Version          uint64         `db:"version"`
	ServletContainer sql.NullString `db:"servlet_container"`
	CountForMonth    uint64         `db:"count_for_month"`
}

// PluginReport is an individual plugin seen in a particular instance report
type PluginReport struct {
	ID       uint64 `db:"id"`
	ReportID uint64 `db:"report_id"`
	PluginID uint64 `db:"plugin_id"`
}

// JobReport is an individual job type and count seen in a particular instance report
type JobReport struct {
	ID        uint64 `db:"id"`
	ReportID  uint64 `db:"report_id"`
	JobTypeID uint64 `db:"job_type_id"`
	Count     uint64 `db:"count"`
}

// NodeReport is an individual node seen in a particular instance report
type NodeReport struct {
	ID           uint64         `db:"id"`
	ReportID     uint64         `db:"report_id"`
	OSID         uint64         `db:"os_id"`
	JVMVersionID uint64         `db:"jvm_version_id"`
	Executors    uint64         `db:"executors"`
	JVMName      sql.NullString `db:"jvm_name"`
	JVMVendor    sql.NullString `db:"jvm_vendor"`
	IsController bool           `db:"is_controller"`
}

type dbInterface interface {
	Get(dest interface{}, query string, args ...interface{}) error
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Preparex(query string) (*sqlx.Stmt, error)
}

// GetJVMVersionID gets the ID for the row of this version if it exists, and creates it and returns the ID if not
func GetJVMVersionID(db dbInterface, name string) (uint64, error) {
	var row JVMVersion
	err := db.Get(&row, "SELECT * FROM jvm_versions where name = $1", name)
	if err == sql.ErrNoRows {
		stmt, err := db.Preparex("INSERT INTO jvm_versions (name) VALUES ($1) RETURNING id")
		if err != nil {
			return 0, err
		}
		var id int
		err = stmt.Get(&id, name)
		if err != nil {
			return 0, err
		}
		return uint64(id), nil
	}
	if err == nil {
		return row.ID, nil
	}
	return 0, err
}

// GetOSTypeID gets the ID for the row of this OS if it exists, and creates it and returns the ID if not
func GetOSTypeID(db dbInterface, name string) (uint64, error) {
	var row OSType
	err := db.Get(&row, "SELECT * FROM os_types where name = $1", name)
	if err == sql.ErrNoRows {
		stmt, err := db.Preparex("INSERT INTO os_types (name) VALUES ($1) RETURNING id")
		if err != nil {
			return 0, err
		}
		var id int
		err = stmt.Get(&id, name)
		if err != nil {
			return 0, err
		}
		return uint64(id), nil
	}
	if err == nil {
		return row.ID, nil
	}
	return 0, err
}

// GetJobTypeID gets the ID for the row of this job type if it exists, and creates it and returns the ID if not
func GetJobTypeID(db dbInterface, name string) (uint64, error) {
	var row JobType
	err := db.Get(&row, "SELECT * FROM job_types where name = $1", name)
	if err == sql.ErrNoRows {
		stmt, err := db.Preparex("INSERT INTO job_types (name) VALUES ($1) RETURNING id")
		if err != nil {
			return 0, err
		}
		var id int
		err = stmt.Get(&id, name)
		if err != nil {
			return 0, err
		}
		return uint64(id), nil
	}
	if err == nil {
		return row.ID, nil
	}
	return 0, err
}

// GetJenkinsVersionID gets the ID for the row of this version if it exists, and creates it and returns the ID if not
func GetJenkinsVersionID(db dbInterface, version string) (uint64, error) {
	var row JenkinsVersion
	err := db.Get(&row, "SELECT * FROM jenkins_versions where version = $1", version)
	if err == sql.ErrNoRows {
		stmt, err := db.Preparex("INSERT INTO jenkins_versions (version) VALUES ($1) RETURNING id")
		if err != nil {
			return 0, err
		}
		var id int
		err = stmt.Get(&id, version)
		if err != nil {
			return 0, err
		}
		return uint64(id), nil
	}
	if err == nil {
		return row.ID, nil
	}
	return 0, err
}

// GetPluginID gets the ID for the row of this plugin/version if it exists, and creates it and returns the ID if not
func GetPluginID(db dbInterface, name, version string) (uint64, error) {
	var row Plugin
	err := db.Get(&row, "SELECT * FROM plugins where name = $1 and version = $2", name, version)
	if err == sql.ErrNoRows {
		stmt, err := db.Preparex("INSERT INTO plugins (name, version) VALUES ($1, $2) RETURNING id")
		if err != nil {
			return 0, err
		}
		var id int
		err = stmt.Get(&id, name, version)
		if err != nil {
			return 0, err
		}
		return uint64(id), nil
	}
	if err == nil {
		return row.ID, nil
	}
	return 0, err
}

// AddReport adds/updates the JSON report to the database, along with all related tables.
func AddReport(db *sqlx.DB, jsonReport *JSONReport) error {
	ts, err := jsonReport.Timestamp()
	if err != nil {
		return err
	}

	insertRow := false

	// Check if there's an existing report.
	var report InstanceReport
	err = db.Get(&report, "SELECT * FROM instance_reports WHERE instance_id = $1 and year = $2 and month = $3",
		jsonReport.Install,
		ts.Year(),
		ts.Month())
	if err != nil {
		if err == sql.ErrNoRows {
			insertRow = true
			report.InstanceID = jsonReport.Install
			report.Year = uint16(ts.Year())
			report.Month = uint16(ts.Month())
		} else {
			return err
		}
	}

	// If we already have a report for this install at this time, skip it.
	if report.ReportTime == ts {
		return nil
	}

	report.ReportTime = ts
	if jsonReport.ServletContainer != "" {
		report.ServletContainer = sql.NullString{String: jsonReport.ServletContainer}
	}

	jvID, err := GetJenkinsVersionID(db, jsonReport.Version)
	if err != nil {
		return err
	}
	report.Version = jvID

	tx, err := db.Beginx()
	if err != nil {
		return err
	}

	if insertRow {
		stmt, err := tx.Preparex("INSERT INTO instance_reports (instance_id, report_time, year, month, version, servlet_container, count_for_month) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id")
		if err != nil {
			return handleErrorInTx(db, err)
		}
		var insertID int
		err = stmt.Get(&insertID,
			report.InstanceID,
			report.ReportTime,
			report.Year,
			report.Month,
			report.Version,
			report.ServletContainer,
			1)
		if err != nil {
			return handleErrorInTx(db, err)
		}
		report.ID = uint64(insertID)
	} else {
		_, err = tx.Exec(`UPDATE instance_reports
SET report_time = $1,
    version = $2,
    servlet_container = $3,
    count_for_month = $4
WHERE id = $5
`, report.ReportTime, report.Version, report.ServletContainer, report.CountForMonth+1, report.ID)
		if err != nil {
			return handleErrorInTx(db, err)
		}

		// Delete the existing plugin, job, and node reports for the existing report ID
		_, err = tx.Exec("DELETE FROM plugin_reports WHERE report_id = $1", report.ID)
		if err != nil {
			return handleErrorInTx(db, err)
		}
		_, err = tx.Exec("DELETE FROM job_reports WHERE report_id = $1", report.ID)
		if err != nil {
			return handleErrorInTx(db, err)
		}
		_, err = tx.Exec("DELETE FROM nodes WHERE report_id = $1", report.ID)
		if err != nil {
			return handleErrorInTx(db, err)
		}
	}

	for _, jsonPlugin := range jsonReport.Plugins {
		pluginID, err := GetPluginID(tx, jsonPlugin.Name, jsonPlugin.Version)
		if err != nil {
			return handleErrorInTx(db, err)
		}
		_, err = tx.Exec("INSERT INTO plugin_reports (report_id, plugin_id) VALUES ($1, $2)", report.ID, pluginID)
		if err != nil {
			return handleErrorInTx(db, err)
		}
	}

	for jobType, count := range jsonReport.Jobs {
		if count != 0 {
			jobTypeID, err := GetJobTypeID(tx, jobType)
			if err != nil {
				return handleErrorInTx(db, err)
			}
			_, err = tx.Exec("INSERT INTO job_reports (report_id, job_type_id, count) VALUES ($1, $2, $3)", report.ID, jobTypeID, count)
			if err != nil {
				return handleErrorInTx(db, err)
			}
		}
	}

	for _, jsonNode := range jsonReport.Nodes {
		jvmVersionID, err := GetJVMVersionID(tx, jsonNode.JVMVersion)
		if err != nil {
			return handleErrorInTx(db, err)
		}
		osTypeID, err := GetOSTypeID(tx, jsonNode.OS)
		if err != nil {
			return handleErrorInTx(db, err)
		}
		_, err = tx.Exec("INSERT INTO nodes (report_id, os_id, jvm_version_id, executors, jvm_name, jvm_vendor, is_controller) VALUES ($1, $2, $3, $4, $5, $6, $7)",
			report.ID,
			osTypeID,
			jvmVersionID,
			jsonNode.Executors,
			jsonNode.JVMName,
			jsonNode.JVMVendor,
			jsonNode.IsController)
		if err != nil {
			return handleErrorInTx(db, err)
		}
	}

	return tx.Commit()
}

func handleErrorInTx(db dbInterface, dbErr error) error {
	var errs error
	errs = multierror.Append(errs, dbErr)
	tx, ok := db.(*sqlx.Tx)
	if ok {
		err := tx.Rollback()
		if err != nil {
			errs = multierror.Append(errs, err)
		}
	}

	return errs
}

// GetInstallCountForVersions generates a map of Jenkins versions to install counts
func GetInstallCountForVersions(db sq.BaseRunner, year, month string) (map[string]uint64, error) {
	rows, err := PSQL().Select("jenkins_versions.version as version", "count(*) as number").
		From("instance_reports").
		Join("jenkins_versions on instance_reports.version = jenkins_versions.id").
		Where(sq.Eq{"instance_reports.year": year}).
		Where(sq.Eq{"instance_reports.month": month}).
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
	rows, err := PSQL().Select("plugins.name as name", "count(*) as number").
		From("plugin_reports").
		Join("plugins on plugin_reports.plugin_id = plugins.id").
		Join("instance_reports on plugin_reports.report_id = instance_reports.id").
		Where(sq.Eq{"instance_reports.year": year}).
		Where(sq.Eq{"instance_reports.month": month}).
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

// PSQL is a postgresql squirrel statement builder
func PSQL() sq.StatementBuilderType {
	return sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
}
