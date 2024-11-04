package main

import (
	"fmt"
	"os"
	"time"

	stats "github.com/jenkins-infra/jenkins-usage-stats"
	"github.com/spf13/cobra"
)

// ReportOptions contains the configuration for actually outputting reports
type ReportOptions struct {
	Directory   string
	Database    string
	LatestYear  int
	LatestMonth int
}

// NewReportCmd returns the report command
func NewReportCmd() *cobra.Command {
	options := &ReportOptions{}

	cobraCmd := &cobra.Command{
		Use:   "report",
		Short: "Generate stats.jenkins.io reports",
		Run: func(_ *cobra.Command, args []string) {
			if err := options.runReport(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
		DisableAutoGenTag: true,
	}

	cobraCmd.Flags().StringVar(&options.Database, "database", "", "Database URL to import to")
	_ = cobraCmd.MarkFlagRequired("database")
	cobraCmd.Flags().StringVar(&options.Directory, "directory", "", "Directory to output to")
	_ = cobraCmd.MarkFlagRequired("directory")
	cobraCmd.Flags().IntVar(&options.LatestYear, "latest-year", 0, "Year of latest data to include. Defaults to the year of the previous month of when this is running.")
	cobraCmd.Flags().IntVar(&options.LatestMonth, "latest-month", 0, "Month of latest data to include. Defaults the previous month of when this is running.")
	cobraCmd.MarkFlagsRequiredTogether("latest-year", "latest-month")

	return cobraCmd
}

func (ro *ReportOptions) runReport() error {
	db, closeFunc, err := getDatabase(ro.Database)
	if err != nil {
		return err
	}
	defer closeFunc()

	startTime := time.Now()
	err = stats.GenerateReport(db, ro.LatestYear, ro.LatestMonth, ro.Directory)
	if err != nil {
		return err
	}

	fmt.Printf("Reports generated to %s, in %s\n", ro.Directory, time.Since(startTime))
	return nil
}
