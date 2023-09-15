package cmdline

import (
	"fmt"
	"time"
)

func CacheMovePackage(ctx CommandContext, packageID int64, nodeID int64) error {
	taskID, err := ctx.Cmdline.Svc.CacheSvc().StartCacheMovePackage(0, packageID, nodeID)
	if err != nil {
		return fmt.Errorf("start cache moving package: %w", err)
	}

	for {
		complete, _, err := ctx.Cmdline.Svc.CacheSvc().WaitCacheMovePackage(nodeID, taskID, time.Second*10)
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

func init() {
	commands.Add(CacheMovePackage, "cache", "move")
}
