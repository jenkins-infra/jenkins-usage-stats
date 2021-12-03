package stats_test

import (
	"testing"
	"time"

	stats "github.com/jenkins-infra/jenkins-usage-stats"
	"github.com/stretchr/testify/assert"
)

func TestTimestampFuncs(t *testing.T) {
	testCases := []struct {
		orig        string
		reorganized string
		timestamp   time.Time
		err         error
	}{
		{
			orig:        "14/Feb/2008:03:44:55 +0000",
			reorganized: "2008-02-14T03:44:55+00:00",
			timestamp:   time.Date(2008, time.February, 14, 3, 44, 55, 0, time.UTC),
		},
		{
			orig:        "02/Mar/2014:21:23:59 -0700",
			reorganized: "2014-03-02T21:23:59-07:00",
			timestamp:   time.Date(2014, time.March, 3, 4, 23, 59, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.orig, func(t *testing.T) {
			assert.Equal(t, tc.reorganized, stats.JSONTimestampToRFC3339(tc.orig))
			r := &stats.JSONReport{TimestampString: tc.orig}
			ts, err := r.Timestamp()
			if tc.err != nil {
				assert.Equal(t, tc.err, err)
			} else {
				assert.Equal(t, tc.timestamp, ts)
			}
		})
	}
}
