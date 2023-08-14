package utils

import (
	"fmt"
	"strings"
)

// MakeMoveOperationFileName Move操作时，写入的文件的名称
func MakeMoveOperationFileName(objectID int64, userID int64) string {
	return fmt.Sprintf("%d-%d", objectID, userID)
}

// GetDirectoryName 根据objectName获取所属的文件夹名
func GetDirectoryName(objectName string) string {
	parts := strings.Split(objectName, "/")
	//若为文件，dirName设置为空
	if len(parts) == 1 {
		return ""
	}
	return parts[0]
}
