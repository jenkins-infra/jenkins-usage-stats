package pkg

import (
	"fmt"
	"regexp"
	"time"
)

const (
	jsonReportDateRE = `(\d\d)\/(\w\w\w)\/(\d\d\d\d)\:(\d\d\:\d\d\:\d\d) ([\+\-]\d\d)(\d\d)`
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

// JSONNode is how a node report is represented in the JSON
type JSONNode struct {
	Executors    uint64 `json:"executors,omitempty"`
	JVMName      string `json:"jvm-name,omitempty"`
	JVMVendor    string `json:"jvm-vendor,omitempty"`
	JVMVersion   string `json:"jvm-version,omitempty"`
	IsController bool   `json:"master"`
	OS           string `json:"os,omitempty"`
}

// JSONPlugin is how a plugin report is represented in the JSON
type JSONPlugin struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// JSONReport is how an instance report is represented in the JSON
type JSONReport struct {
	Install          string            `json:"install"`
	Jobs             map[string]uint64 `json:"jobs"`
	Nodes            []JSONNode        `json:"nodes"`
	Plugins          []JSONPlugin      `json:"plugins"`
	ServletContainer string            `json:"servletContainer,omitempty"`
	TimestampString  string            `json:"timestamp"`
	Version          string            `json:"version"`
}

// Timestamp parses the raw timestamp string on a report
func (j *JSONReport) Timestamp() (time.Time, error) {
	return time.Parse(time.RFC3339, JSONTimestampToRFC3339(j.TimestampString))
}

// JSONTimestampToRFC3339 converts the timestamp string in the raw reports into a form Go can parse
func JSONTimestampToRFC3339(ts string) string {
	re := regexp.MustCompile(jsonReportDateRE)
	matches := re.FindAllStringSubmatch(ts, -1)
	return fmt.Sprintf("%s-%s-%sT%s%s:%s", matches[0][3], shortMonthToNumber[matches[0][2]], matches[0][1], matches[0][4], matches[0][5], matches[0][6])
	/*	withoutZone := strings.TrimSuffix(ts, " +0000")
		splitDateAndTime := strings.SplitN(withoutZone, ":", 2)
		dayMonthYear := strings.Split(splitDateAndTime[0], "/")
		return fmt.Sprintf("%s-%s-%sT%sZ", dayMonthYear[2], shortMonthToNumber[dayMonthYear[1]], dayMonthYear[0], splitDateAndTime[1])*/
}
