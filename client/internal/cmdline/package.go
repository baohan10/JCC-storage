package cmdline

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jedib0t/go-pretty/v6/table"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

func PackageListBucketPackages(ctx CommandContext, bucketID cdssdk.BucketID) error {
	userID := cdssdk.UserID(1)

	packages, err := ctx.Cmdline.Svc.BucketSvc().GetBucketPackages(userID, bucketID)
	if err != nil {
		return err
	}

	fmt.Printf("Find %d packages in bucket %d for user %d:\n", len(packages), bucketID, userID)

	tb := table.NewWriter()
	tb.AppendHeader(table.Row{"ID", "Name", "BucketID", "State"})

	for _, obj := range packages {
		tb.AppendRow(table.Row{obj.PackageID, obj.Name, obj.BucketID, obj.State})
	}

	fmt.Println(tb.Render())
	return nil
}

func PackageDownloadPackage(ctx CommandContext, packageID cdssdk.PackageID, outputDir string) error {
	userID := cdssdk.UserID(1)

	err := os.MkdirAll(outputDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("create output directory %s failed, err: %w", outputDir, err)
	}

	// 下载文件
	objIter, err := ctx.Cmdline.Svc.PackageSvc().DownloadPackage(userID, packageID)
	if err != nil {
		return fmt.Errorf("download object failed, err: %w", err)
	}
	defer objIter.Close()

	madeDirs := make(map[string]bool)

	for {
		objInfo, err := objIter.MoveNext()
		if err == iterator.ErrNoMoreItem {
			break
		}
		if err != nil {
			return err
		}

		err = func() error {
			defer objInfo.File.Close()

			fullPath := filepath.Join(outputDir, objInfo.Object.Path)

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
			return err
		}
	}

	return nil
}

func PackageCreatePackage(ctx CommandContext, bucketID cdssdk.BucketID, name string) error {
	userID := cdssdk.UserID(1)

	pkgID, err := ctx.Cmdline.Svc.PackageSvc().Create(userID, bucketID, name)
	if err != nil {
		return err
	}

	fmt.Printf("%v\n", pkgID)
	return nil
}

func PackageDeletePackage(ctx CommandContext, packageID cdssdk.PackageID) error {
	userID := cdssdk.UserID(1)
	err := ctx.Cmdline.Svc.PackageSvc().DeletePackage(userID, packageID)
	if err != nil {
		return fmt.Errorf("delete package %d failed, err: %w", packageID, err)
	}
	return nil
}

func PackageGetCachedNodes(ctx CommandContext, packageID cdssdk.PackageID) error {
	userID := cdssdk.UserID(1)
	resp, err := ctx.Cmdline.Svc.PackageSvc().GetCachedNodes(userID, packageID)
	fmt.Printf("resp: %v\n", resp)
	if err != nil {
		return fmt.Errorf("get package %d cached nodes failed, err: %w", packageID, err)
	}
	return nil
}

func PackageGetLoadedNodes(ctx CommandContext, packageID cdssdk.PackageID) error {
	userID := cdssdk.UserID(1)
	nodeIDs, err := ctx.Cmdline.Svc.PackageSvc().GetLoadedNodes(userID, packageID)
	fmt.Printf("nodeIDs: %v\n", nodeIDs)
	if err != nil {
		return fmt.Errorf("get package %d loaded nodes failed, err: %w", packageID, err)
	}
	return nil
}

func init() {
	commands.MustAdd(PackageListBucketPackages, "pkg", "ls")

	commands.MustAdd(PackageDownloadPackage, "pkg", "get")

	commands.MustAdd(PackageCreatePackage, "pkg", "new")

	commands.MustAdd(PackageDeletePackage, "pkg", "delete")

	commands.MustAdd(PackageGetCachedNodes, "pkg", "cached")

	commands.MustAdd(PackageGetLoadedNodes, "pkg", "loaded")
}
