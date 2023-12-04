package cmdline

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

func PackageListBucketPackages(ctx CommandContext, bucketID cdssdk.BucketID) error {
	userID := cdssdk.UserID(0)

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

func PackageDownloadPackage(ctx CommandContext, outputDir string, packageID cdssdk.PackageID) error {
	err := os.MkdirAll(outputDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("create output directory %s failed, err: %w", outputDir, err)
	}

	// 下载文件
	objIter, err := ctx.Cmdline.Svc.PackageSvc().DownloadPackage(0, packageID)
	if err != nil {
		return fmt.Errorf("download object failed, err: %w", err)
	}
	defer objIter.Close()

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
			if err := os.MkdirAll(dirPath, 0755); err != nil {
				return fmt.Errorf("creating object dir: %w", err)
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

func PackageCreatePackage(ctx CommandContext, rootPath string, bucketID cdssdk.BucketID, name string, nodeAffinity []cdssdk.NodeID) error {
	rootPath = filepath.Clean(rootPath)

	var uploadFilePathes []string
	err := filepath.WalkDir(rootPath, func(fname string, fi os.DirEntry, err error) error {
		if err != nil {
			return nil
		}

		if !fi.IsDir() {
			uploadFilePathes = append(uploadFilePathes, fname)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("open directory %s failed, err: %w", rootPath, err)
	}

	var nodeAff *cdssdk.NodeID
	if len(nodeAffinity) > 0 {
		n := cdssdk.NodeID(nodeAffinity[0])
		nodeAff = &n
	}

	objIter := iterator.NewUploadingObjectIterator(rootPath, uploadFilePathes)
	taskID, err := ctx.Cmdline.Svc.PackageSvc().StartCreatingPackage(0, bucketID, name, objIter, nodeAff)

	if err != nil {
		return fmt.Errorf("upload file data failed, err: %w", err)
	}

	for {
		complete, uploadObjectResult, err := ctx.Cmdline.Svc.PackageSvc().WaitCreatingPackage(taskID, time.Second*5)
		if complete {
			if err != nil {
				return fmt.Errorf("uploading package: %w", err)
			}

			tb := table.NewWriter()

			tb.AppendHeader(table.Row{"Path", "ObjectID"})
			for i := 0; i < len(uploadObjectResult.ObjectResults); i++ {
				tb.AppendRow(table.Row{
					uploadObjectResult.ObjectResults[i].Info.Path,
					uploadObjectResult.ObjectResults[i].ObjectID,
				})
			}
			fmt.Print(tb.Render())
			return nil
		}

		if err != nil {
			return fmt.Errorf("wait uploading: %w", err)
		}
	}
}

func PackageUpdatePackage(ctx CommandContext, packageID cdssdk.PackageID, rootPath string) error {
	//userID := int64(0)

	var uploadFilePathes []string
	err := filepath.WalkDir(rootPath, func(fname string, fi os.DirEntry, err error) error {
		if err != nil {
			return nil
		}

		if !fi.IsDir() {
			uploadFilePathes = append(uploadFilePathes, fname)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("open directory %s failed, err: %w", rootPath, err)
	}

	objIter := iterator.NewUploadingObjectIterator(rootPath, uploadFilePathes)
	taskID, err := ctx.Cmdline.Svc.PackageSvc().StartUpdatingPackage(0, packageID, objIter)
	if err != nil {
		return fmt.Errorf("update package %d failed, err: %w", packageID, err)
	}

	for {
		complete, _, err := ctx.Cmdline.Svc.PackageSvc().WaitUpdatingPackage(taskID, time.Second*5)
		if complete {
			if err != nil {
				return fmt.Errorf("updating package: %w", err)
			}

			return nil
		}

		if err != nil {
			return fmt.Errorf("wait updating: %w", err)
		}
	}
}

func PackageDeletePackage(ctx CommandContext, packageID cdssdk.PackageID) error {
	userID := cdssdk.UserID(0)
	err := ctx.Cmdline.Svc.PackageSvc().DeletePackage(userID, packageID)
	if err != nil {
		return fmt.Errorf("delete package %d failed, err: %w", packageID, err)
	}
	return nil
}

func PackageGetCachedNodes(ctx CommandContext, packageID cdssdk.PackageID, userID cdssdk.UserID) error {
	resp, err := ctx.Cmdline.Svc.PackageSvc().GetCachedNodes(userID, packageID)
	fmt.Printf("resp: %v\n", resp)
	if err != nil {
		return fmt.Errorf("get package %d cached nodes failed, err: %w", packageID, err)
	}
	return nil
}

func PackageGetLoadedNodes(ctx CommandContext, packageID cdssdk.PackageID, userID cdssdk.UserID) error {
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

	commands.MustAdd(PackageUpdatePackage, "pkg", "update")

	commands.MustAdd(PackageDeletePackage, "pkg", "delete")

	commands.MustAdd(PackageGetCachedNodes, "pkg", "cached")

	commands.MustAdd(PackageGetLoadedNodes, "pkg", "loaded")
}
