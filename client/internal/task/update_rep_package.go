package task

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/cmd"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

type UpdateRepPackageResult = cmd.UpdateRepPackageResult

type UpdateRepPackage struct {
	cmd cmd.UpdateRepPackage

	Result *UpdateRepPackageResult
}

func NewUpdateRepPackage(userID int64, packageID int64, objectIter iterator.UploadingObjectIterator) *UpdateRepPackage {
	return &UpdateRepPackage{
		cmd: *cmd.NewUpdateRepPackage(userID, packageID, objectIter),
	}
}

func (t *UpdateRepPackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	ret, err := t.cmd.Execute(&cmd.UpdatePackageContext{
		Distlock: ctx.distlock,
	})

	t.Result = ret

	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
