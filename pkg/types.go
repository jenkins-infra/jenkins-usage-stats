package pkg

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

var (
	shortMonthToNumber = map[string]string{
		"Jan": "01",
		"Feb": "02",
		"Mar": "03",
		"Apr": "04",
		"May": "05",
		"Jun": "06",
		"Jul": "07",
		"Aug": "08",
		"Sep": "09",
		"Oct": "10",
		"Nov": "11",
		"Dec": "12",
	}
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
	JVMVersion   uint64         `db:"jvm_version"`
	Executors    uint64         `db:"executors"`
	JVMName      sql.NullString `db:"jvm_name"`
	JVMVendor    sql.NullString `db:"jvm_vendor"`
	IsController bool           `db:"is_controller"`
}

type JSONNode struct {
	Executors    uint64 `json:"executors,omitempty"`
	JVMName      string `json:"jvm-name,omitempty"`
	JVMVendor    string `json:"jvm-vendor,omitempty"`
	JVMVersion   string `json:"jvm-version,omitempty"`
	IsController bool   `json:"master"`
	OS           string `json:"os,omitempty"`
}

type JSONPlugin struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type JSONReport struct {
	Install          string            `json:"install"`
	Jobs             map[string]uint64 `json:"jobs"`
	Nodes            []JSONNode        `json:"nodes"`
	Plugins          []JSONPlugin      `json:"plugins"`
	ServletContainer string            `json:"servletContainer,omitempty"`
	TimestampString  string            `json:"timestamp"`
	Version          string            `json:"version"`
}

func (j *JSONReport) Timestamp() (time.Time, error) {
	return time.Parse(time.RFC3339, JSONTimestampToRFC3339(j.TimestampString))
}

func JSONTimestampToRFC3339(ts string) string {
	withoutZone := strings.TrimSuffix(ts, " +0000")
	splitDateAndTime := strings.SplitN(withoutZone, ":", 2)
	dayMonthYear := strings.Split(splitDateAndTime[0], "/")
	return fmt.Sprintf("%s-%s-%sT%sZ", dayMonthYear[2], shortMonthToNumber[dayMonthYear[1]], dayMonthYear[0], splitDateAndTime[1])
}
