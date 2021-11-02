package pkg

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
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
		reports = append(reports, r)
	}

	return reports, nil
}
