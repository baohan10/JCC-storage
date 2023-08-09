package cmdline

import (
	"fmt"
	"time"
)

func StorageMoveObject(ctx CommandContext, objectID int64, storageID int64) error {
	taskID, err := ctx.Cmdline.Svc.StorageSvc().StartStorageMoveObject(0, objectID, storageID)
	if err != nil {
		return fmt.Errorf("start moving object to storage: %w", err)
	}

	for {
		complete, err := ctx.Cmdline.Svc.StorageSvc().WaitStorageMoveObjectToStorage(taskID, time.Second*10)
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

func StorageMoveDir(ctx CommandContext, dirName string, storageID int64) error {
	taskID, err := ctx.Cmdline.Svc.StorageSvc().StartMovingObjectDirToStorage(0, dirName, storageID)
	if err != nil {
		return fmt.Errorf("start moving object to storage: %w", err)
	}

	for {
		complete, results, err := ctx.Cmdline.Svc.StorageSvc().WaitMovingObjectDirToStorage(taskID, time.Second*5)
		if complete {
			if err != nil {
				return fmt.Errorf("moving complete with: %w", err)
			}
			// 返回各object的move结果
			for _, result := range results {
				if result.Error != nil {
					fmt.Printf("moving %s to storage failed: %s\n", result.ObjectName, result.Error)
				}
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("wait moving: %w", err)
		}
	}
}

func StorageUploadRepObject(ctx CommandContext, storageID int64, filePath string, bucketID int64, objectName string, repCount int) error {
	nodeID, taskID, err := ctx.Cmdline.Svc.StorageSvc().StartStorageUploadRepObject(0, storageID, filePath, bucketID, objectName, repCount)
	if err != nil {
		return fmt.Errorf("start storage uploading rep object: %w", err)
	}

	for {
		complete, objectID, fileHash, err := ctx.Cmdline.Svc.StorageSvc().WaitStorageUploadRepObject(nodeID, taskID, time.Second*10)
		if complete {
			if err != nil {
				return fmt.Errorf("uploading complete with: %w", err)
			}

			fmt.Printf("%d\n%s\n", objectID, fileHash)
			return nil
		}

		if err != nil {
			return fmt.Errorf("wait uploading: %w", err)
		}
	}
}

func init() {
	commands.MustAdd(StorageMoveObject, "storage", "move", "obj")

	commands.MustAdd(StorageMoveDir, "storage", "move", "dir")

	commands.MustAdd(StorageUploadRepObject, "storage", "upload", "rep")
}
