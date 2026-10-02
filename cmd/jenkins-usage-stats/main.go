package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	sq "github.com/Masterminds/squirrel"

	"github.com/spf13/cobra"

	"github.com/jenkins-infra/jenkins-usage-stats/pkg/version"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	rootCmd := &cobra.Command{
		Use:   "jenkins-usage-stats",
		Version: version.Version,
		Short: "Command for running the Jenkins usage stats import and report generation",
		Run: func(cmd *cobra.Command, _ []string) {
			_ = cmd.Help()
		},
		DisableAutoGenTag: true,
	}

	rootCmd.AddCommand(NewImportCmd())
	rootCmd.AddCommand(NewReportCmd())
	rootCmd.AddCommand(NewFetchCmd(ctx))

	return rootCmd.Execute()
}

func getDatabase(dbURL string) (sq.DBProxyBeginner, func(), error) {
	rawDB, err := sql.Open("postgres", dbURL)
	if err != nil {
		return nil, nil, err
	}

	return sq.NewStmtCacheProxy(rawDB), func() {
		_ = rawDB.Close()
	}, nil
}
