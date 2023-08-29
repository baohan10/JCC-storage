package task

import (
	"time"

	"gitlink.org.cn/cloudream/storage/common/pkgs/cmd"
)

type DownloadPackage struct {
	cmd *cmd.DownloadPackage
}

func NewDownloadPackage(userID int64, packageID int64, outputPath string) *DownloadPackage {
	return &DownloadPackage{
		cmd: cmd.NewDownloadPackage(userID, packageID, outputPath),
	}
}
func (t *DownloadPackage) Execute(ctx TaskContext, complete CompleteFn) {
	err := t.cmd.Execute(&cmd.DownloadPackageContext{
		Distlock: ctx.distlock,
	})

	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
