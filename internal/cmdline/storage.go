package cmdline

import (
	"fmt"
	"time"
)

func StorageMoveObjectToStorage(ctx CommandContext, objectID int64, storageID int64) error {
	taskID, err := ctx.Cmdline.Svc.StorageSvc().StartMovingObjectToStorage(0, objectID, storageID)
	if err != nil {
		return fmt.Errorf("start moving object to storage: %w", err)
	}

	for {
		complete, err := ctx.Cmdline.Svc.StorageSvc().WaitMovingObjectToStorage(taskID, time.Second*5)
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

func StorageMoveObjectDirToStorage(ctx CommandContext, dirName string, storageID int64) error {

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

func init() {
	commands.MustAdd(StorageMoveObjectToStorage, "storage", "move", "obj")
	commands.MustAdd(StorageMoveObjectDirToStorage, "storage", "move", "dir")
}
