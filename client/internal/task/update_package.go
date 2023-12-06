package task

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/task"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/cmd"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

type UpdatePackageResult = cmd.UpdatePackageResult

type UpdatePackage struct {
	cmd cmd.UpdatePackage

	Result *UpdatePackageResult
}

func NewUpdatePackage(userID cdssdk.UserID, packageID cdssdk.PackageID, objectIter iterator.UploadingObjectIterator) *UpdatePackage {
	return &UpdatePackage{
		cmd: *cmd.NewUpdatePackage(userID, packageID, objectIter),
	}
}

func (t *UpdatePackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	ret, err := t.cmd.Execute(&cmd.UpdatePackageContext{
		Distlock: ctx.distlock,
	})

	t.Result = ret

	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
