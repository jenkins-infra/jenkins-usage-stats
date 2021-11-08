//go:build integration
// +build integration

package pkg_test

import (
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/abayer/jenkins-usage-stats/pkg"
	"github.com/stretchr/testify/require"
)

func TestDBIntegration(t *testing.T) {
	db, closeFunc := DBForTest(t)
	defer closeFunc()

	cache := pkg.NewStatsCache()

	sampleStatsDir := filepath.Join("testdata", "2021-stats")
	files, err := ioutil.ReadDir(sampleStatsDir)
	require.NoError(t, err)

	totalReports := 0

	for _, fi := range files {
		if !fi.IsDir() && strings.HasSuffix(fi.Name(), ".gz") { //&& strings.Contains(fi.Name(), "200901") {
			startedAt := time.Now()
			alreadyRead, err := pkg.ReportAlreadyRead(db, fi.Name())
			require.NoError(t, err)
			if alreadyRead {
				t.Logf("file %s already read\n", fi.Name())
				continue
			}
			fn := filepath.Join(sampleStatsDir, fi.Name())
			jsonReports, err := pkg.ParseDailyJSON(fn)
			require.NoError(t, err)
			t.Logf("adding %d reports from file %s\n", len(jsonReports), fi.Name())
			totalReports += len(jsonReports)
			for _, jr := range jsonReports {
				require.NoError(t, pkg.AddReport(db, cache, jr))
			}
			t.Logf("imported in %s", time.Since(startedAt))
		}
	}

	t.Log(cache.ReportTimes())
	t.Logf("total reports: %d\n", totalReports)
}
