package pkg_test

import (
	"path/filepath"
	"testing"

	"github.com/abayer/jenkins-usage-stats/pkg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDailyJSON(t *testing.T) {
	fooFile := filepath.Join("testdata", "foo.json.gz")

	reports, err := pkg.ParseDailyJSON(fooFile)
	require.NoError(t, err)
	assert.Len(t, reports, 2)
	assert.Equal(t, "32b68faa8644852c4ad79540b4bfeb1caf63284811f4f9d6c2bc511f797218c8", reports[0].Install)
	assert.Equal(t, uint64(50), reports[0].Jobs["hudson-maven-MavenModuleSet"])
	assert.Len(t, reports[0].Plugins, 75)
}
