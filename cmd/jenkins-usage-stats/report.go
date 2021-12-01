package main

import (
	"context"
	"fmt"
	"os"
	"time"

	stats "github.com/abayer/jenkins-usage-stats"
	"github.com/spf13/cobra"
)

// ReportOptions contains the configuration for actually outputting reports
type ReportOptions struct {
	Directory string
	Database  string
}

// NewReportCmd returns the report command
func NewReportCmd(ctx context.Context) *cobra.Command {
	options := &ReportOptions{}

	cobraCmd := &cobra.Command{
		Use:   "report",
		Short: "Generate stats.jenkins.io reports",
		Run: func(cmd *cobra.Command, args []string) {
			if err := options.runReport(ctx); err != nil {
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

	return cobraCmd
}

func (ro *ReportOptions) runReport(ctx context.Context) error {
	db, closeFunc, err := getDatabase(ro.Database)
	if err != nil {
		return err
	}
	defer closeFunc()

	now := time.Now()

	startTime := time.Now()
	err = stats.GenerateReport(db, now.Year(), int(now.Month()), ro.Directory)
	if err != nil {
		return err
	}

	fmt.Printf("Reports generated to %s, in %s\n", ro.Directory, time.Since(startTime))
	return nil
}
