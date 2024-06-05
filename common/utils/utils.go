package utils

import (
	"path/filepath"
	"strconv"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func MakeLoadedPackagePath(userID cdssdk.UserID, packageID cdssdk.PackageID) string {
	return filepath.Join("packages", strconv.FormatInt(int64(userID), 10), strconv.FormatInt(int64(packageID), 10))
}

func MakeStorageLoadDirectory(stgDir string) string {
	return filepath.Join(stgDir, "packages")
}
