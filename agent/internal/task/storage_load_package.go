package task

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/task"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/cmd"
)

type StorageLoadPackage struct {
	cmd      *cmd.DownloadPackage
	FullPath string
}

func NewStorageLoadPackage(userID cdssdk.UserID, packageID cdssdk.PackageID, outputPath string) *StorageLoadPackage {
	return &StorageLoadPackage{
		cmd:      cmd.NewDownloadPackage(userID, packageID, outputPath),
		FullPath: outputPath,
	}
}
func (t *StorageLoadPackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	err := t.cmd.Execute(&cmd.DownloadPackageContext{
		Distlock: ctx.distlock,
	})

	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
