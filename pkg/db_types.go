package pkg

import (
	"database/sql"
	"time"

	"github.com/hashicorp/go-multierror"

	"github.com/jmoiron/sqlx"
)

type JVMVersion struct {
	ID   uint64 `db:"id"`
	Name string `db:"name"`
}

type OSType struct {
	ID   uint64 `db:"id"`
	Name string `db:"name"`
}

type JobType struct {
	ID   uint64 `db:"id"`
	Name string `db:"name"`
}

type Plugin struct {
	ID      uint64 `db:"id"`
	Name    string `db:"name"`
	Version string `db:"version"`
}

type JenkinsVersion struct {
	ID      uint64 `db:"id"`
	Version string `db:"version"`
}

type InstanceReport struct {
	ID               uint64         `db:"id"`
	InstanceID       string         `db:"instance_id"`
	ReportTime       time.Time      `db:"report_time"`
	Year             uint8          `db:"year"`
	Month            uint8          `db:"month"`
	Version          uint64         `db:"version"`
	ServletContainer sql.NullString `db:"servlet_container"`
	CountForMonth    uint64         `db:"count_for_month"`
}

type PluginReport struct {
	ID       uint64 `db:"id"`
	ReportID uint64 `db:"report_id"`
	PluginID uint64 `db:"plugin_id"`
}

type JobReport struct {
	ID        uint64 `db:"id"`
	ReportID  uint64 `db:"report_id"`
	JobTypeID uint64 `db:"job_type_id"`
	Count     uint64 `db:"count"`
}

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
}

func GetJVMVersionID(db dbInterface, name string) (uint64, error) {
	var row *JVMVersion
	err := db.Get(&row, "SELECT * FROM jvm_versions where name = ?", name)
	if err == sql.ErrNoRows {
		result, err := db.Exec("INSERT INTO jvm_versions (name) VALUES (?) RETURNING id", name)
		if err != nil {
			return 0, err
		}
		id, err := result.LastInsertId()
		if err != nil {
			return 0, err
		}
		return uint64(id), nil
	}
	if err != nil {
		return row.ID, nil
	}
	return 0, err
}

func GetOSTypeID(db dbInterface, name string) (uint64, error) {
	var row *OSType
	err := db.Get(&row, "SELECT * FROM os_types where name = ?", name)
	if err == sql.ErrNoRows {
		result, err := db.Exec("INSERT INTO os_types (name) VALUES (?) RETURNING id", name)
		if err != nil {
			return 0, err
		}
		id, err := result.LastInsertId()
		if err != nil {
			return 0, err
		}
		return uint64(id), nil
	}
	if err != nil {
		return row.ID, nil
	}
	return 0, err
}

func GetJobTypeID(db dbInterface, name string) (uint64, error) {
	var row *JobType
	err := db.Get(&row, "SELECT * FROM job_types where name = ?", name)
	if err == sql.ErrNoRows {
		result, err := db.Exec("INSERT INTO job_types (name) VALUES (?) RETURNING id", name)
		if err != nil {
			return 0, err
		}
		id, err := result.LastInsertId()
		if err != nil {
			return 0, err
		}
		return uint64(id), nil
	}
	if err != nil {
		return row.ID, nil
	}
	return 0, err
}

func GetJenkinsVersionID(db dbInterface, version string) (uint64, error) {
	var row *JenkinsVersion
	err := db.Get(&row, "SELECT * FROM jenkins_versions where version = ?", version)
	if err == sql.ErrNoRows {
		result, err := db.Exec("INSERT INTO jenkins_versions (version) VALUES (?) RETURNING id", version)
		if err != nil {
			return 0, err
		}
		id, err := result.LastInsertId()
		if err != nil {
			return 0, err
		}
		return uint64(id), nil
	}
	if err != nil {
		return row.ID, nil
	}
	return 0, err
}

func GetPluginID(db dbInterface, name, version string) (uint64, error) {
	var row *Plugin
	err := db.Get(&row, "SELECT * FROM plugins where name = ? and version = ?", name, version)
	if err == sql.ErrNoRows {
		result, err := db.Exec("INSERT INTO plugins (name, version) VALUES (?, ?) RETURNING id", name, version)
		if err != nil {
			return 0, err
		}
		id, err := result.LastInsertId()
		if err != nil {
			return 0, err
		}
		return uint64(id), nil
	}
	if err != nil {
		return row.ID, nil
	}
	return 0, err
}

func AddReport(db *sqlx.DB, jsonReport *JSONReport) error {
	ts, err := jsonReport.Timestamp()
	if err != nil {
		return err
	}

	insertRow := false

	// Check if there's an existing report.
	var report *InstanceReport
	err = db.Get(&report, "SELECT * FROM instance_reports WHERE instance_id = ? and year = ? and month = ?",
		jsonReport.Install,
		ts.Year(),
		ts.Month())
	if err != nil {
		if err == sql.ErrNoRows {
			insertRow = true
			report.InstanceID = jsonReport.Install
			report.Year = uint8(ts.Year())
			report.Month = uint8(ts.Month())
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
		result, err := tx.Exec("INSERT INTO instance_reports (instance_id, report_time, year, month, version, servlet_container, count_for_month) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id",
			report.InstanceID,
			report.ReportTime,
			report.Year,
			report.Month,
			report.Version,
			report.ServletContainer,
			1)
		if err != nil {
			return handleDBError(db, err)
		}
		insertID, err := result.LastInsertId()
		if err != nil {
			return handleDBError(db, err)
		}
		report.ID = uint64(insertID)
	} else {
		_, err = tx.Exec(`UPDATE instance_reports
SET report_time = ?,
    version = ?,
    servlet_container = ?,
    count_for_month = ?
WHERE id = ?
`, report.ReportTime, report.Version, report.ServletContainer, report.CountForMonth+1, report.ID)
		if err != nil {
			return handleDBError(db, err)
		}

		// Delete the existing plugin, job, and node reports for the existing report ID
		_, err = tx.Exec("DELETE FROM plugin_reports WHERE report_id = ?", report.ID)
		if err != nil {
			return handleDBError(db, err)
		}
		_, err = tx.Exec("DELETE FROM job_reports WHERE report_id = ?", report.ID)
		if err != nil {
			return handleDBError(db, err)
		}
		_, err = tx.Exec("DELETE FROM nodes WHERE report_id = ?", report.ID)
		if err != nil {
			return handleDBError(db, err)
		}
	}

	for _, jsonPlugin := range jsonReport.Plugins {
		pluginID, err := GetPluginID(tx, jsonPlugin.Name, jsonPlugin.Version)
		if err != nil {
			return handleDBError(db, err)
		}
		_, err = tx.Exec("INSERT INTO plugin_reports (report_id, plugin_id) VALUES (?, ?)", report.ID, pluginID)
		if err != nil {
			return handleDBError(db, err)
		}
	}

	for jobType, count := range jsonReport.Jobs {
		if count != 0 {
			jobTypeID, err := GetJobTypeID(tx, jobType)
			if err != nil {
				return handleDBError(db, err)
			}
			_, err = tx.Exec("INSERT INTO job_reports (report_id, job_type_id, count) VALUES (?, ?, ?)", report.ID, jobTypeID, count)
			if err != nil {
				return handleDBError(db, err)
			}
		}
	}

	for _, jsonNode := range jsonReport.Nodes {
		jvmVersionID, err := GetJVMVersionID(tx, jsonNode.JVMVersion)
		if err != nil {
			return handleDBError(db, err)
		}
		osTypeID, err := GetOSTypeID(tx, jsonNode.OS)
		if err != nil {
			return handleDBError(db, err)
		}
		_, err = tx.Exec("INSERT INTO nodes (report_id, os_id, jvm_version_id, executors, jvm_name, jvm_vendor, is_controller) VALUES (?, ?, ?, ?, ?, ?, ?)",
			report.ID,
			osTypeID,
			jvmVersionID,
			jsonNode.Executors,
			jsonNode.JVMName,
			jsonNode.JVMVendor,
			jsonNode.IsController)
		if err != nil {
			return handleDBError(db, err)
		}
	}

	return tx.Commit()
}

func handleDBError(db dbInterface, dbErr error) error {
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

func GetInstallCountForVersions(db dbInterface, year, month string) (map[string]uint64, error) {
	results, err := db.Query("SELECT jenkins_versions.version, COUNT(*) AS number FROM jenkins_versions inner join instance_reports on jenkins_versions.id = instance_reports.version WHERE instance_reports.year=? AND instance_reports.month=? GROUP BY jenkins_versions.version", year, month)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = results.Close()
	}()

	versionMap := make(map[string]uint64)
	for results.Next() {
		var v string
		var c uint64
		err := results.Scan(&v, &c)
		if err != nil {
			return nil, err
		}
		versionMap[v] = c
	}

	return versionMap, nil
}
