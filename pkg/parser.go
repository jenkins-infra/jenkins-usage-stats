package pkg

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
)

func ParseDailyJSON(filename string) ([]*JSONReport, error) {
	gzippedJSON, err := ioutil.ReadFile(filename)
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
		fmt.Printf("line: %s\n", scanner.Text())
		err = json.Unmarshal(scanner.Bytes(), &r)
		if err != nil {
			return nil, err
		}
		FilterPrivateFromReport(r)
		reports = append(reports, r)
	}

	return reports, nil
}

func FilterPrivateFromReport(r *JSONReport) {
	var plugins []JSONPlugin
	for _, p := range r.Plugins {
		if !strings.HasPrefix(p.Name, "privateplugin-") && !strings.Contains(p.Version, "(private)") {
			plugins = append(plugins, p)
		}
	}
	r.Plugins = plugins
}
