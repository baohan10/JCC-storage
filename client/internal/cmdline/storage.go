package cmdline

import (
	"fmt"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func StorageLoadPackage(ctx CommandContext, packageID int64, storageID int64) error {
	taskID, err := ctx.Cmdline.Svc.StorageSvc().StartStorageLoadPackage(0, packageID, storageID)
	if err != nil {
		return fmt.Errorf("start loading package to storage: %w", err)
	}

	for {
		complete, fullPath, err := ctx.Cmdline.Svc.StorageSvc().WaitStorageLoadPackage(taskID, time.Second*10)
		if complete {
			if err != nil {
				return fmt.Errorf("moving complete with: %w", err)
			}

			fmt.Printf("Load To: %s\n", fullPath)
			return nil
		}

		if err != nil {
			return fmt.Errorf("wait moving: %w", err)
		}
	}
}

func StorageCreateRepPackage(ctx CommandContext, bucketID int64, name string, storageID int64, path string, repCount int) error {
	nodeID, taskID, err := ctx.Cmdline.Svc.StorageSvc().StartStorageCreatePackage(0, bucketID, name, storageID, path,
		cdssdk.NewTypedRepRedundancyInfo(repCount), nil)
	if err != nil {
		return fmt.Errorf("start storage uploading rep package: %w", err)
	}

	for {
		complete, packageID, err := ctx.Cmdline.Svc.StorageSvc().WaitStorageCreatePackage(nodeID, taskID, time.Second*10)
		if complete {
			if err != nil {
				return fmt.Errorf("uploading complete with: %w", err)
			}

			fmt.Printf("%d\n", packageID)
			return nil
		}

		if err != nil {
			return fmt.Errorf("wait uploading: %w", err)
		}
	}
}

func init() {
	commands.MustAdd(StorageLoadPackage, "stg", "load", "pkg")

	commands.MustAdd(StorageCreateRepPackage, "stg", "upload", "rep")
}
