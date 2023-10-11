package utils

import (
	"path/filepath"
	"strconv"
)

// MakeStorageLoadPackagePath Load操作时，写入的文件夹的名称
func MakeStorageLoadPackagePath(stgDir string, userID int64, packageID int64) string {
	return filepath.Join(stgDir, strconv.FormatInt(userID, 10), "packages", strconv.FormatInt(packageID, 10))
}
