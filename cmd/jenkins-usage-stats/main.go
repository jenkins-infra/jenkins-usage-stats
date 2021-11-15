package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
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
		Short: "Command for running the Jenkins usage stats import and report generation",
		Run: func(cmd *cobra.Command, args []string) {
			_ = cmd.Help()
		},
		DisableAutoGenTag: true,
	}

	rootCmd.AddCommand(NewImportCmd(ctx))
	rootCmd.AddCommand(NewReportCmd(ctx))

	return rootCmd.Execute()
}
