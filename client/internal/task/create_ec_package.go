package task

import (
	"time"

	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	"gitlink.org.cn/cloudream/storage/common/pkgs/cmd"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

type CreateECPackageResult = cmd.CreateECPackageResult

type CreateECPackage struct {
	cmd cmd.CreateECPackage

	Result *CreateECPackageResult
}

func NewCreateECPackage(userID int64, bucketID int64, name string, objIter iterator.UploadingObjectIterator, redundancy models.ECRedundancyInfo, nodeAffinity *int64) *CreateECPackage {
	return &CreateECPackage{
		cmd: *cmd.NewCreateECPackage(userID, bucketID, name, objIter, redundancy, nodeAffinity),
	}
}

func (t *CreateECPackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	ret, err := t.cmd.Execute(&cmd.UpdatePackageContext{
		Distlock: ctx.distlock,
	})
	t.Result = ret

	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
