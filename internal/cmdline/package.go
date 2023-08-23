package cmdline

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/storage-common/pkgs/iterator"
)

func PackageListBucketPackages(ctx CommandContext, bucketID int64) error {
	userID := int64(0)

	packages, err := ctx.Cmdline.Svc.BucketSvc().GetBucketPackages(userID, bucketID)
	if err != nil {
		return err
	}

	fmt.Printf("Find %d packages in bucket %d for user %d:\n", len(packages), bucketID, userID)

	tb := table.NewWriter()
	tb.AppendHeader(table.Row{"ID", "Name", "BucketID", "State", "Redundancy"})

	for _, obj := range packages {
		tb.AppendRow(table.Row{obj.PackageID, obj.Name, obj.BucketID, obj.State, obj.Redundancy})
	}

	fmt.Print(tb.Render())
	return nil
}

func PackageDownloadPackage(ctx CommandContext, outputDir string, packageID int64) error {
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
	}

	return nil
}

func PackageUploadRepPackage(ctx CommandContext, rootPath string, bucketID int64, name string, repCount int) error {
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

	objIter := iterator.NewUploadingObjectIterator(rootPath, uploadFilePathes)
	taskID, err := ctx.Cmdline.Svc.PackageSvc().StartCreatingRepPackage(0, bucketID, name, objIter, models.NewRepRedundancyInfo(repCount))

	if err != nil {
		return fmt.Errorf("upload file data failed, err: %w", err)
	}

	for {
		complete, uploadObjectResult, err := ctx.Cmdline.Svc.PackageSvc().WaitCreatingRepPackage(taskID, time.Second*5)
		if complete {
			if err != nil {
				return fmt.Errorf("uploading rep object: %w", err)
			}

			tb := table.NewWriter()

			tb.AppendHeader(table.Row{"Path", "ObjectID", "FileHash"})
			for i := 0; i < len(uploadObjectResult.ObjectResults); i++ {
				tb.AppendRow(table.Row{
					uploadObjectResult.ObjectResults[i].Info.Path,
					uploadObjectResult.ObjectResults[i].ObjectID,
					uploadObjectResult.ObjectResults[i].FileHash,
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

func PackageUpdateRepPackage(ctx CommandContext, packageID int64, rootPath string) error {
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
	taskID, err := ctx.Cmdline.Svc.PackageSvc().StartUpdatingRepPackage(0, packageID, objIter)
	if err != nil {
		return fmt.Errorf("update object %d failed, err: %w", packageID, err)
	}

	for {
		complete, _, err := ctx.Cmdline.Svc.PackageSvc().WaitUpdatingRepPackage(taskID, time.Second*5)
		if complete {
			if err != nil {
				return fmt.Errorf("updating rep object: %w", err)
			}

			return nil
		}

		if err != nil {
			return fmt.Errorf("wait updating: %w", err)
		}
	}
}

func PackageUploadECPackage(ctx CommandContext, rootPath string, bucketID int64, name string, ecName string) error {
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
	taskID, err := ctx.Cmdline.Svc.PackageSvc().StartCreatingECPackage(0, bucketID, name, objIter, models.NewECRedundancyInfo(ecName))

	if err != nil {
		return fmt.Errorf("upload file data failed, err: %w", err)
	}

	for {
		complete, uploadObjectResult, err := ctx.Cmdline.Svc.PackageSvc().WaitCreatingRepPackage(taskID, time.Second*5)
		if complete {
			if err != nil {
				return fmt.Errorf("uploading ec package: %w", err)
			}

			tb := table.NewWriter()

			tb.AppendHeader(table.Row{"Path", "ObjectID", "FileHash"})
			for i := 0; i < len(uploadObjectResult.ObjectResults); i++ {
				tb.AppendRow(table.Row{
					uploadObjectResult.ObjectResults[i].Info.Path,
					uploadObjectResult.ObjectResults[i].ObjectID,
					uploadObjectResult.ObjectResults[i].FileHash,
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

func PackageUpdateECPackage(ctx CommandContext, packageID int64, rootPath string) error {
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
	taskID, err := ctx.Cmdline.Svc.PackageSvc().StartUpdatingECPackage(0, packageID, objIter)
	if err != nil {
		return fmt.Errorf("update package %d failed, err: %w", packageID, err)
	}

	for {
		complete, _, err := ctx.Cmdline.Svc.PackageSvc().WaitUpdatingECPackage(taskID, time.Second*5)
		if complete {
			if err != nil {
				return fmt.Errorf("updating ec package: %w", err)
			}

			return nil
		}

		if err != nil {
			return fmt.Errorf("wait updating: %w", err)
		}
	}
}

func PackageDeletePackage(ctx CommandContext, packageID int64) error {
	userID := int64(0)
	err := ctx.Cmdline.Svc.PackageSvc().DeletePackage(userID, packageID)
	if err != nil {
		return fmt.Errorf("delete package %d failed, err: %w", packageID, err)
	}
	return nil
}

func init() {
	commands.MustAdd(PackageListBucketPackages, "pkg", "ls")

	commands.MustAdd(PackageDownloadPackage, "pkg", "get")

	commands.MustAdd(PackageUploadRepPackage, "pkg", "new", "rep")

	commands.MustAdd(PackageUpdateRepPackage, "pkg", "update", "rep")

	commands.MustAdd(PackageUploadRepPackage, "pkg", "new", "ec")

	commands.MustAdd(PackageUpdateRepPackage, "pkg", "update", "ec")

	commands.MustAdd(PackageDeletePackage, "pkg", "delete")
}
