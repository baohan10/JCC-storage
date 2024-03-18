package task

import (
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/task"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/cmd"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
)

type UploadObjectsResult = cmd.UploadObjectsResult

type UploadObjects struct {
	cmd cmd.UploadObjects

	Result *UploadObjectsResult
}

func NewUploadObjects(userID cdssdk.UserID, packageID cdssdk.PackageID, objectIter iterator.UploadingObjectIterator, nodeAffinity *cdssdk.NodeID) *UploadObjects {
	return &UploadObjects{
		cmd: *cmd.NewUploadObjects(userID, packageID, objectIter, nodeAffinity),
	}
}

func (t *UploadObjects) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	ret, err := t.cmd.Execute(&cmd.UploadObjectsContext{
		Distlock: ctx.distlock,
	})

	t.Result = ret

	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
