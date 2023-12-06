package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type DownloadPackage struct {
	userID     cdssdk.UserID
	packageID  cdssdk.PackageID
	outputPath string
}

type DownloadPackageContext struct {
	Distlock *distlock.Service
}

func NewDownloadPackage(userID cdssdk.UserID, packageID cdssdk.PackageID, outputPath string) *DownloadPackage {
	return &DownloadPackage{
		userID:     userID,
		packageID:  packageID,
		outputPath: outputPath,
	}
}

func (t *DownloadPackage) Execute(ctx *DownloadPackageContext) error {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getObjectDetails, err := coorCli.GetPackageObjectDetails(coormq.NewGetPackageObjectDetails(t.packageID))
	if err != nil {
		return fmt.Errorf("getting package object details: %w", err)
	}

	objIter := iterator.NewDownloadObjectIterator(getObjectDetails.Objects, &iterator.DownloadContext{
		Distlock: ctx.Distlock,
	})
	defer objIter.Close()

	return t.writeObjects(objIter)
}

func (t *DownloadPackage) writeObjects(objIter iterator.DownloadingObjectIterator) error {
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

			fullPath := filepath.Join(t.outputPath, objInfo.Object.Path)

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
