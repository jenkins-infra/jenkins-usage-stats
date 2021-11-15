//go:build integration

package stats_test

import (
	"database/sql"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	stats "github.com/abayer/jenkins-usage-stats"
	"github.com/abayer/jenkins-usage-stats/testutil"
	"github.com/stretchr/testify/require"
)

func TestDBIntegration(t *testing.T) {
	db, closeFunc := DBForIntTest(t)
	defer closeFunc()

	cache := stats.NewStatsCache()

	sampleStatsDir := filepath.Join("testdata", "report-stats")
	files, err := ioutil.ReadDir(sampleStatsDir)
	require.NoError(t, err)

	totalReports := 0

	dateRe := regexp.MustCompile(`.*\.(\d\d\d\d\d\d).*`)

	sort.Slice(files, func(i, j int) bool {
		iMatch := dateRe.FindStringSubmatch(files[i].Name())
		iDate := iMatch[1]
		if iDate == "" {
			return true
		}
		jMatch := dateRe.FindStringSubmatch(files[j].Name())
		jDate := jMatch[1]
		if jDate == "" {
			return true
		}
		return iDate < jDate
	})

	for _, fi := range files {
		if !fi.IsDir() && !strings.Contains(fi.Name(), "fudged") && strings.HasSuffix(fi.Name(), ".gz") { // && strings.Contains(fi.Name(), ".201001") {
			startedAt := time.Now()
			alreadyRead, err := stats.ReportAlreadyRead(db, fi.Name())
			require.NoError(t, err)
			if alreadyRead {
				t.Logf("file %s already read\n", fi.Name())
				continue
			}
			fn := filepath.Join(sampleStatsDir, fi.Name())
			jsonReports, err := stats.ParseDailyJSON(fn)
			require.NoError(t, err)
			t.Logf("adding %d reports from file %s\n", len(jsonReports), fi.Name())
			totalReports += len(jsonReports)
			for _, jr := range jsonReports {
				require.NoError(t, stats.AddIndividualReport(db, cache, jr))
			}
			require.NoError(t, stats.MarkReportRead(db, fi.Name()))
			t.Logf("imported in %s", time.Since(startedAt))
		}
	}

	t.Log(cache.ReportTimes())
	t.Logf("total reports: %d\n", totalReports)
}

// DBForIntTest connects to a local database for testing
func DBForIntTest(f testutil.Fataler) (sq.BaseRunner, func()) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres@localhost/jenkins_usage_stats?sslmode=disable&timezone=UTC"
	}

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		f.Fatal(err)
	}

	closeFunc := func() {
		if err := db.Close(); err != nil {
			f.Fatal(err)
		}
	}

	return sq.NewStmtCacheProxy(db), closeFunc
}
