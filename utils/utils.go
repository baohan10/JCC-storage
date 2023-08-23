package utils

import (
	"fmt"
)

// MakeStorageLoadPackageDirName Load操作时，写入的文件夹的名称
func MakeStorageLoadPackageDirName(packageID int64, userID int64) string {
	return fmt.Sprintf("%d-%d", packageID, userID)
}
