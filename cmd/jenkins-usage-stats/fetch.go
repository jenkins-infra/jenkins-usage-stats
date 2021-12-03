package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	stats "github.com/jenkins-infra/jenkins-usage-stats"
	"github.com/spf13/cobra"
)

// FetchOptions is the configuration for the fetch command
type FetchOptions struct {
	Database       string
	Directory      string
	AzureAccount   string
	AzureKey       string
	AzureContainer string
}

// NewFetchCmd returns the fetch command
func NewFetchCmd(ctx context.Context) *cobra.Command {
	options := &FetchOptions{}

	cobraCmd := &cobra.Command{
		Use:   "fetch",
		Short: "Fetch raw usage data from Azure",
		Run: func(cmd *cobra.Command, args []string) {
			if err := options.runFetch(ctx); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		},
		DisableAutoGenTag: true,
	}

	cobraCmd.Flags().StringVar(&options.Database, "database", "", "Database URL")
	_ = cobraCmd.MarkFlagRequired("database")
	cobraCmd.Flags().StringVar(&options.Directory, "directory", "", "Directory to write raw usage gz files to")
	_ = cobraCmd.MarkFlagRequired("directory")
	cobraCmd.Flags().StringVar(&options.AzureAccount, "account", "", "Azure account")
	_ = cobraCmd.MarkFlagRequired("account")
	cobraCmd.Flags().StringVar(&options.AzureKey, "key", "", "Azure key")
	_ = cobraCmd.MarkFlagRequired("key")
	cobraCmd.Flags().StringVar(&options.AzureContainer, "container", "", "Azure blob container")
	_ = cobraCmd.MarkFlagRequired("container")

	return cobraCmd
}

func (fo *FetchOptions) runFetch(ctx context.Context) error {
	db, closeFunc, err := getDatabase(fo.Database)
	if err != nil {
		return err
	}
	defer closeFunc()

	fmt.Printf("creating raw usage directory %s if it doesn't exist\n", fo.Directory)
	err = os.MkdirAll(fo.Directory, 0755) //nolint:gosec
	if err != nil {
		return err
	}

	azCred, err := azblob.NewSharedKeyCredential(fo.AzureAccount, fo.AzureKey)
	if err != nil {
		return err
	}

	azClient, err := azblob.NewServiceClientWithSharedKey(fmt.Sprintf("https://%s.blob.core.windows.net/", fo.AzureAccount), azCred, nil)
	if err != nil {
		return err
	}

	var toDownload []string

	azCtr := azClient.NewContainerClient(fo.AzureContainer)

	fmt.Printf("checking container %s for new raw usage files\n", fo.AzureContainer)
	pager := azCtr.ListBlobsFlat(nil)

	for pager.NextPage(ctx) {
		resp := pager.PageResponse()

		for _, v := range resp.ContainerListBlobFlatSegmentResult.Segment.BlobItems {
			if v.Name != nil {
				// Check if we've already recorded this file in the database.
				alreadyRecorded, err := stats.ReportAlreadyRead(db, *v.Name)
				if err != nil {
					return err
				}
				if !alreadyRecorded {
					fmt.Printf("%s - new raw usage file, queuing for download\n", *v.Name)
					toDownload = append(toDownload, *v.Name)
				}
			}
		}
	}

	if err := pager.Err(); err != nil {
		return err
	}

	if len(toDownload) == 0 {
		fmt.Println("no new raw usage files to download, finishing")
		return nil
	}

	fmt.Printf("%d new raw usage files to download\n", len(toDownload))

	for _, fn := range toDownload {
		blockBlob := azCtr.NewBlobClient(fn)

		fmt.Printf(" - downloading %s\n", fn)
		dlResp, err := blockBlob.Download(ctx, nil)
		if err != nil {
			return err
		}

		dlData, err := ioutil.ReadAll(dlResp.Body(azblob.RetryReaderOptions{}))
		if err != nil {
			return err
		}

		destFile := filepath.Join(fo.Directory, fn)
		err = ioutil.WriteFile(destFile, dlData, 0644) //nolint:gosec
		if err != nil {
			return err
		}
	}

	fmt.Println("fetch complete")
	return nil
}
