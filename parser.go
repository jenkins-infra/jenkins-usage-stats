package stats

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"
	"strings"
)

var (
	hotspotAndMisc16Versions = []string{
		// Until sometime in 2010, HotSpot and some other JVMs reported _their_ version number, not the Java version.
		// So for those cases, we're just going to map them all to 1.6.

		// HotSpot versions
		"10.0-b19",
		"10.0-b22",
		"10.0-b23",
		"10.0-b25",
		"11.0-b11",
		"11.0-b12",
		"11.0-b15",
		"11.0-b16",
		"11.0-b17",
		"11.2-b01",
		"11.3-b02",
		"13.0-b04",
		"14.0-b01",
		"14.0-b05",
		"14.0-b08",
		"14.0-b09",
		"14.0-b10",
		"14.0-b12",
		"14.0-b15",
		"14.0-b16",
		"14.1-b02",
		"14.2-b01",
		"14.3-b01",
		"16.0-b03",
		"16.0-b08",
		"16.0-b13",
		"16.2-b04",
		"16.3-b01",
		"17.0-b14",
		"17.0-b15",
		"17.0-b16",
		"17.0-b17",
		"17.1-b03",
		// IBM JVM
		"2.3",
		"2.4",
		// SAP JVM
		"5.1.0844",
		"5.1.0909",
	}
)

// ParseDailyJSON parses an individual day's gzipped JSON reports
func ParseDailyJSON(filename string) ([]*JSONReport, error) {
	gzippedJSON, err := ioutil.ReadFile(filename) // #nosec
	if err != nil {
		return nil, err
	}
	zReader, err := gzip.NewReader(bytes.NewReader(gzippedJSON))
	if err != nil {
		return nil, err
	}

	var reports []*JSONReport

	scanner := bufio.NewScanner(zReader)
	sBuffer := make([]byte, 0, bufio.MaxScanTokenSize)
	scanner.Buffer(sBuffer, bufio.MaxScanTokenSize*50) // Otherwise long lines crash the scanner.

	for scanner.Scan() {
		var r *JSONReport
		err = json.Unmarshal(scanner.Bytes(), &r)
		if err != nil {
			// If the error is a "cannot unmarshal number...", just skip this record. This is to deal with the range of
			// possible weird executor count values we see, ranging from -4 to 2147483655 - i.e., 8 more than the max 32
			// bit number. We're opting to just pay attention to positive values, and we don't really want to deal with
			// bad data anyway.
			if strings.Contains(err.Error(), "cannot unmarshal number") {
				continue
			}
			// If the error is "cannot unmarshal array into Go struct field JSONReport.jobs of type uint64", we hit a
			// weird case of the value for a job count being an array, so let's just ignore that record.
			if strings.Contains(err.Error(), "cannot unmarshal array into Go struct field JSONReport.jobs of type uint64") {
				continue
			}
			return nil, err
		}
		FilterPrivateFromReport(r)
		standardizeJVMVersions(r)
		reports = append(reports, r)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return reports, nil
}

// FilterPrivateFromReport removes private plugins from the report
func FilterPrivateFromReport(r *JSONReport) {
	var plugins []JSONPlugin
	for _, p := range r.Plugins {
		if !strings.HasPrefix(p.Name, "privateplugin-") && !strings.Contains(p.Version, "(private)") {
			plugins = append(plugins, p)
		}
	}
	r.Plugins = plugins
}

func standardizeJVMVersions(r *JSONReport) {
	var nodes []JSONNode
	for _, n := range r.Nodes {
		fullVersion := hotspotJVMVersionToJavaVersion(n.JVMVersion)
		if fullVersion == "" {
			n.JVMVersion = "N/A"
		} else if fullVersion == "8" {
			n.JVMVersion = "1.8"
		} else if strings.HasPrefix(fullVersion, "1.") {
			n.JVMVersion = fullVersion[0:3]
			if n.JVMVersion == "1.9" {
				n.JVMVersion = "9"
			}
		} else {
			splitVersion := strings.Split(fullVersion, ".")
			n.JVMVersion = splitVersion[0]
		}
		nodes = append(nodes, n)
	}
	r.Nodes = nodes
}

func hotspotJVMVersionToJavaVersion(input string) string {
	for _, hv := range hotspotAndMisc16Versions {
		if input == hv {
			return "1.6"
		}
	}
	return input
}
