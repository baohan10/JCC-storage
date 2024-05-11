package cmdline

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/inhies/go-bytesize"
	"github.com/spf13/cobra"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

func init() {
	var usePkgID bool
	cmd := &cobra.Command{
		Use:   "getp",
		Short: "Download whole package by package id or path",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			cmdCtx := GetCmdCtx(cmd)

			if usePkgID {
				id, err := strconv.ParseInt(args[0], 10, 64)
				if err != nil {
					fmt.Printf("Invalid package id: %s\n", args[0])
					return
				}

				getpByID(cmdCtx, cdssdk.PackageID(id), args[1])
			} else {
				getpByPath(cmdCtx, args[0], args[1])
			}
		},
	}
	cmd.Flags().BoolVarP(&usePkgID, "id", "i", false, "Download with package id instead of path")

	rootCmd.AddCommand(cmd)
}

func getpByPath(cmdCtx *CommandContext, path string, output string) {
	userID := cdssdk.UserID(1)

	comps := strings.Split(strings.Trim(path, cdssdk.ObjectPathSeparator), cdssdk.ObjectPathSeparator)
	if len(comps) != 2 {
		fmt.Printf("Package path must be in format of <bucket>/<package>")
		return
	}

	pkg, err := cmdCtx.Cmdline.Svc.PackageSvc().GetByName(userID, comps[0], comps[1])
	if err != nil {
		fmt.Println(err)
		return
	}

	getpByID(cmdCtx, pkg.PackageID, output)
}

func getpByID(cmdCtx *CommandContext, id cdssdk.PackageID, output string) {
	userID := cdssdk.UserID(1)
	startTime := time.Now()

	objIter, err := cmdCtx.Cmdline.Svc.PackageSvc().DownloadPackage(userID, id)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = os.MkdirAll(output, os.ModePerm)
	if err != nil {
		fmt.Printf("Create output directory %s failed, err: %v", output, err)
		return
	}
	defer objIter.Close()

	madeDirs := make(map[string]bool)
	fileCount := 0
	totalSize := int64(0)

	for {
		objInfo, err := objIter.MoveNext()
		if err == iterator.ErrNoMoreItem {
			break
		}
		if err != nil {
			fmt.Println(err)
			return
		}

		err = func() error {
			defer objInfo.File.Close()
			fileCount++
			totalSize += objInfo.Object.Size

			fullPath := filepath.Join(output, objInfo.Object.Path)

			dirPath := filepath.Dir(fullPath)
			if !madeDirs[dirPath] {
				if err := os.MkdirAll(dirPath, 0755); err != nil {
					return fmt.Errorf("creating object dir: %w", err)
				}
				madeDirs[dirPath] = true
			}

			outputFile, err := os.Create(fullPath)
			if err != nil {
				return fmt.Errorf("creating object file: %w", err)
			}
			defer outputFile.Close()

			_, err = io.Copy(outputFile, objInfo.File)
			if err != nil {
				return fmt.Errorf("copy object data to local file failed, err: %w", err)
			}

			return nil
		}()
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	fmt.Printf("Get %v files (%v) to %s in %v.\n", fileCount, bytesize.ByteSize(totalSize), output, time.Since(startTime))
}
