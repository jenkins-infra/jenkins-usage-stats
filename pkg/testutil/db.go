package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/docker/go-connections/nat"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	dbName = "jenkins_usage_stats_test"
)

// Fataler interface has a single method Fatal, which takes
// a slice of arguments and is expected to panic.
type Fataler interface {
	Fatal(args ...interface{})
}

// DBForTest spins up a postgres container, creates the test database on it, migrates it, and returns the db and a close function
func DBForTest(f Fataler) (sq.BaseRunner, func()) {
	ctx := context.Background()
	// container and database
	container, db, err := CreateTestContainer(ctx)
	if err != nil {
		f.Fatal(err)
	}

	closeFunc := func() {
		_ = db.Close()
		_ = container.Terminate(ctx)
	}

	// migration
	mig, err := NewPgMigrator(db)
	if err != nil {
		closeFunc()
		f.Fatal(err)
	}

	err = mig.Up()
	if err != nil {
		closeFunc()
		f.Fatal(err)
	}

	return db, closeFunc
}

// CreateTestContainer spins up a Postgres database container
func CreateTestContainer(ctx context.Context) (testcontainers.Container, *sql.DB, error) {
	env := map[string]string{
		"POSTGRES_PASSWORD": "password",
		"POSTGRES_USER":     "postgres",
		"POSTGRES_DB":       dbName,
	}
	dockerPort := "5432/tcp"
	dbURL := func(port nat.Port) string {
		return fmt.Sprintf("postgres://postgres:password@localhost:%s/%s?sslmode=disable", port.Port(), dbName)
	}

	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:12",
			ExposedPorts: []string{dockerPort},
			Cmd:          []string{"postgres", "-c", "fsync=off"},
			Env:          env,
			WaitingFor:   wait.ForSQL(nat.Port(dockerPort), "postgres", dbURL).Timeout(time.Second * 30),
		},
		Started: true,
	}
	container, err := testcontainers.GenericContainer(ctx, req)
	if err != nil {
		return container, nil, fmt.Errorf("failed to start container: %s", err)
	}

	mappedPort, err := container.MappedPort(ctx, nat.Port(dockerPort))
	if err != nil {
		return container, nil, fmt.Errorf("failed to get container external port: %s", err)
	}

	log.Println("postgres container ready and running at dockerPort: ", mappedPort)

	url := fmt.Sprintf("postgres://postgres:password@localhost:%s/%s?sslmode=disable", mappedPort.Port(), dbName)
	db, err := sql.Open("postgres", url)
	if err != nil {
		return container, db, fmt.Errorf("failed to establish database connection: %s", err)
	}

	return container, db, nil
}

// NewPgMigrator creates a migrator instance
func NewPgMigrator(db *sql.DB) (*migrate.Migrate, error) {
	_, path, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatalf("failed to get path")
	}

	sourceURL := "file://" + filepath.Dir(path) + "/../../etc/migrations"

	driver, err := postgres.WithInstance(db, &postgres.Config{})

	if err != nil {
		log.Fatalf("failed to create migrator driver: %s", err)
	}

	m, err := migrate.NewWithDatabaseInstance(sourceURL, "postgres", driver)

	return m, err
}
