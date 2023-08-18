package cmdline

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/models"
)

func StorageMovePackage(ctx CommandContext, packageID int64, storageID int64) error {
	taskID, err := ctx.Cmdline.Svc.StorageSvc().StartStorageMovePackage(0, packageID, storageID)
	if err != nil {
		return fmt.Errorf("start moving package to storage: %w", err)
	}

	for {
		complete, err := ctx.Cmdline.Svc.StorageSvc().WaitStorageMovePackage(taskID, time.Second*10)
		if complete {
			if err != nil {
				return fmt.Errorf("moving complete with: %w", err)
			}

			return nil
		}

		if err != nil {
			return fmt.Errorf("wait moving: %w", err)
		}
	}
}

func StorageCreateRepPackage(ctx CommandContext, bucketID int64, name string, storageID int64, path string, repCount int) error {
	nodeID, taskID, err := ctx.Cmdline.Svc.StorageSvc().StartStorageCreatePackage(0, bucketID, name, storageID, path,
		models.NewTypedRepRedundancyInfo(repCount))
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
	commands.MustAdd(StorageMovePackage, "storage", "move", "pkg")

	commands.MustAdd(StorageCreateRepPackage, "storage", "upload", "rep")
}
