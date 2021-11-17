package stats

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"
	"strings"
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
		fullVersion := n.JVMVersion
		if strings.HasPrefix(fullVersion, "1.") {
			n.JVMVersion = fullVersion[0:3]
		} else {
			splitVersion := strings.Split(fullVersion, ".")
			n.JVMVersion = splitVersion[0]
		}
		nodes = append(nodes, n)
	}
	r.Nodes = nodes
}
