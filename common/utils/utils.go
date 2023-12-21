package utils

import (
	"path/filepath"
	"strconv"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

// MakeStorageLoadPackagePath Load操作时，写入的文件夹的名称
func MakeStorageLoadPackagePath(stgDir string, userID cdssdk.UserID, packageID cdssdk.PackageID) string {
	return filepath.Join(stgDir, strconv.FormatInt(int64(userID), 10), "packages", strconv.FormatInt(int64(packageID), 10))
}

func MakeStorageLoadDirectory(stgDir string, userIDStr string) string {
	return filepath.Join(stgDir, userIDStr, "packages")
}
