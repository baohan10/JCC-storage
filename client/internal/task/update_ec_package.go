package task

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/task"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/cmd"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

type UpdateECPackageResult = cmd.UpdateECPackageResult

type UpdateECPackage struct {
	cmd cmd.UpdateECPackage

	Result *UpdateECPackageResult
}

func NewUpdateECPackage(userID cdssdk.UserID, packageID cdssdk.PackageID, objectIter iterator.UploadingObjectIterator) *UpdateECPackage {
	return &UpdateECPackage{
		cmd: *cmd.NewUpdateECPackage(userID, packageID, objectIter),
	}
}

func (t *UpdateECPackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	ret, err := t.cmd.Execute(&cmd.UpdatePackageContext{
		Distlock: ctx.distlock,
	})

	t.Result = ret

	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
