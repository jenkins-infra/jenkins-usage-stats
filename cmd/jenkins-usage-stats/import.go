package main

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	stats "github.com/abayer/jenkins-usage-stats"
	"github.com/spf13/cobra"
)

// ImportOptions is the configuration for the import command
type ImportOptions struct {
	Database  string
	Directory string
}

// NewImportCmd returns the import command
func NewImportCmd(ctx context.Context) *cobra.Command {
	options := &ImportOptions{}

	cobraCmd := &cobra.Command{
		Use:   "import",
		Short: "Import instance reports",
		Run: func(cmd *cobra.Command, args []string) {
			if err := options.runImport(ctx); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
		DisableAutoGenTag: true,
	}

	cobraCmd.Flags().StringVar(&options.Database, "database", "", "Database URL to import to")
	_ = cobraCmd.MarkFlagRequired("database")
	cobraCmd.Flags().StringVar(&options.Directory, "directory", "", "Directory to import from")
	_ = cobraCmd.MarkFlagRequired("directory")

	return cobraCmd
}

func (io *ImportOptions) runImport(ctx context.Context) error {
	rawDB, err := sql.Open("postgres", io.Database)
	if err != nil {
		return err
	}
	defer func() {
		_ = rawDB.Close()
	}()

	db := sq.NewStmtCacheProxy(rawDB)

	files, err := ioutil.ReadDir(io.Directory)
	if err != nil {
		return err
	}

	totalReports := 0

	dateRe := regexp.MustCompile(`.*\.(\d\d\d\d\d\d\d\d).*`)

	sort.Slice(files, func(i, j int) bool {
		if !files[i].IsDir() && strings.HasSuffix(files[i].Name(), ".gz") && !files[j].IsDir() && strings.HasSuffix(files[j].Name(), ".gz") {
			iMatch := dateRe.FindStringSubmatch(files[i].Name())
			if len(iMatch) > 1 {
				iDate := iMatch[1]
				if iDate == "" {
					return true
				}
				jMatch := dateRe.FindStringSubmatch(files[j].Name())
				if len(jMatch) > 1 {
					jDate := jMatch[1]
					if jDate == "" {
						return true
					}
					return iDate < jDate
				}
			}
		}
		return true
	})

	cache := stats.NewStatsCache()

	importStart := time.Now()

	for _, fi := range files {
		if !fi.IsDir() && strings.HasSuffix(fi.Name(), ".gz") {
			startedAt := time.Now()
			alreadyRead, err := stats.ReportAlreadyRead(db, fi.Name())
			if err != nil {
				return err
			}
			if alreadyRead {
				fmt.Printf("file %s already read\n", fi.Name())
				continue
			}
			fn := filepath.Join(io.Directory, fi.Name())
			jsonReports, err := stats.ParseDailyJSON(fn)
			if err != nil {
				return err
			}
			fmt.Printf("adding %d reports from file %s\n", len(jsonReports), fi.Name())
			totalReports += len(jsonReports)
			for _, jr := range jsonReports {
				if err := stats.AddIndividualReport(db, cache, jr); err != nil {
					return err
				}
			}
			if err := stats.MarkReportRead(db, fi.Name()); err != nil {
				return err
			}
			fmt.Printf("imported in %s\n", time.Since(startedAt))
		}
	}

	fmt.Println(cache.ReportTimes())
	fmt.Printf("total reports: %d (time to import: %s)\n", totalReports, time.Since(importStart))

	return nil
}
