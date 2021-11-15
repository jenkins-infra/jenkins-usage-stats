package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

func main() {
	// capture SIGINT and SIGTERM for graceful shutdown
	kill := make(chan os.Signal, 1)
	signal.Notify(kill, syscall.SIGINT, syscall.SIGTERM)

	defer func() {
		signal.Stop(kill)
		close(kill)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		// wait for kill signal and cancel context
		<-kill
		fmt.Println("exiting...")
		cancel()
	}()

	if err := run(ctx); err != nil {
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

	return rootCmd.Execute()
}
