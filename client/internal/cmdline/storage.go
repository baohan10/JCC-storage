package cmdline

import (
	"fmt"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func StorageLoadPackage(ctx CommandContext, packageID cdssdk.PackageID, storageID cdssdk.StorageID) error {
	startTime := time.Now()
	defer func() {
		fmt.Printf("%v\n", time.Since(startTime).Seconds())
	}()

	nodeID, taskID, err := ctx.Cmdline.Svc.StorageSvc().StartStorageLoadPackage(1, packageID, storageID)
	if err != nil {
		return fmt.Errorf("start loading package to storage: %w", err)
	}

	for {
		complete, fullPath, err := ctx.Cmdline.Svc.StorageSvc().WaitStorageLoadPackage(nodeID, taskID, time.Second*10)
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

func StorageCreatePackage(ctx CommandContext, bucketID cdssdk.BucketID, name string, storageID cdssdk.StorageID, path string) error {
	startTime := time.Now()
	defer func() {
		fmt.Printf("%v\n", time.Since(startTime).Seconds())
	}()

	nodeID, taskID, err := ctx.Cmdline.Svc.StorageSvc().StartStorageCreatePackage(1, bucketID, name, storageID, path, nil)
	if err != nil {
		return fmt.Errorf("start storage uploading package: %w", err)
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
	commands.MustAdd(StorageLoadPackage, "stg", "pkg", "load")

	commands.MustAdd(StorageCreatePackage, "stg", "pkg", "new")
}
