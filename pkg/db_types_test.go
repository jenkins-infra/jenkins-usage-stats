package pkg_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sq "github.com/Masterminds/squirrel"
	"github.com/abayer/jenkins-usage-stats/pkg"
	"github.com/jmoiron/sqlx"
)

func TestGetJVMVersionID(t *testing.T) {
	db, closeFunc := DBForTest(t)
	defer closeFunc()

	firstVer := "1.7"
	//	secondVer := "13"

	var fetchedVersion pkg.JVMVersion
	err := db.Get(&fetchedVersion, "SELECT * FROM jvm_versions WHERE name = $1", firstVer)
	require.Equal(t, sql.ErrNoRows, err)

	firstID, err := pkg.GetJVMVersionID(db, firstVer)
	require.NoError(t, err)
	require.NoError(t, db.Get(&fetchedVersion, "SELECT * FROM jvm_versions WHERE name = $1", firstVer))
	assert.Equal(t, firstID, fetchedVersion.ID)
}

// Fataler interface has a single method Fatal, which takes
// a slice of arguments and is expected to panic.
type Fataler interface {
	Fatal(args ...interface{})
}

// DBForTest connects to a local database for testing
func DBForTest(f Fataler) (*sqlx.DB, func()) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://localhost/jenkins_usage_stats?sslmode=disable&timezone=UTC"
	}

	db, err := sqlx.Open("postgres", databaseURL)
	if err != nil {
		f.Fatal(err)
	}

	closeFunc := func() {
		if err := db.Close(); err != nil {
			f.Fatal(err)
		}
	}

	if err := TruncateAll(db); err != nil {
		f.Fatal(err)
	}
	return db, closeFunc
}

// TruncateAll takes a database connection, lists all the tables which
// aren't tracking schema_migrations and issues a cascading truncate
// across each of them.
func TruncateAll(db *sqlx.DB) error {
	rows, err := pkg.PSQL().
		Select("tablename").
		From("pg_tables").
		Where(sq.Eq{"schemaname": "public"}).
		Where(sq.NotEq{"tablename": "schema_migrations"}).
		RunWith(db).
		Query()
	if err != nil {
		return err
	}

	var tables []string
	for rows.Next() {
		var tablename string
		if err := rows.Scan(&tablename); err != nil {
			return err
		}

		tables = append(tables, tablename)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	for _, table := range tables {
		truncate := fmt.Sprintf(`TRUNCATE TABLE %q CASCADE;`, table)
		if _, err := db.Exec(truncate); err != nil {
			return err
		}

		log.Println(truncate)
	}

	return nil
}
