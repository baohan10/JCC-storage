package task

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/cmd"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

type CreatePackageResult = cmd.CreatePackageResult

type CreatePackage struct {
	cmd cmd.CreatePackage

	Result *CreatePackageResult
}

func NewCreatePackage(userID cdssdk.UserID, bucketID cdssdk.BucketID, name string, objIter iterator.UploadingObjectIterator, nodeAffinity *cdssdk.NodeID) *CreatePackage {
	return &CreatePackage{
		cmd: *cmd.NewCreatePackage(userID, bucketID, name, objIter, nodeAffinity),
	}
}

func (t *CreatePackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	log := logger.WithType[CreatePackage]("Task")
	log.Debugf("begin")
	defer log.Debugf("end")

	ret, err := t.cmd.Execute(&cmd.UpdatePackageContext{
		Distlock: ctx.distlock,
	})
	t.Result = ret

	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
