package task

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/cmd"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	"gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type CreatePackageResult struct {
	PackageID cdssdk.PackageID
	Objects   []cmd.ObjectUploadResult
}

type CreatePackage struct {
	userID       cdssdk.UserID
	bucketID     cdssdk.BucketID
	name         string
	objIter      iterator.UploadingObjectIterator
	nodeAffinity *cdssdk.NodeID
	Result       *CreatePackageResult
}

func NewCreatePackage(userID cdssdk.UserID, bucketID cdssdk.BucketID, name string, objIter iterator.UploadingObjectIterator, nodeAffinity *cdssdk.NodeID) *CreatePackage {
	return &CreatePackage{
		userID:       userID,
		bucketID:     bucketID,
		name:         name,
		objIter:      objIter,
		nodeAffinity: nodeAffinity,
	}
}

func (t *CreatePackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	log := logger.WithType[CreatePackage]("Task")
	log.Debugf("begin")
	defer log.Debugf("end")

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		err = fmt.Errorf("new coordinator client: %w", err)
		log.Warn(err.Error())
		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	createResp, err := coorCli.CreatePackage(coordinator.NewCreatePackage(t.userID, t.bucketID, t.name))
	if err != nil {
		err = fmt.Errorf("creating package: %w", err)
		log.Error(err.Error())
		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}

	uploadRet, err := cmd.NewUploadObjects(t.userID, createResp.Package.PackageID, t.objIter, t.nodeAffinity).Execute(&cmd.UploadObjectsContext{
		Distlock:     ctx.distlock,
		Connectivity: ctx.connectivity,
	})
	if err != nil {
		err = fmt.Errorf("uploading objects: %w", err)
		log.Error(err.Error())
		complete(err, CompleteOption{
			RemovingDelay: time.Minute,
		})
		return
	}

	t.Result.PackageID = createResp.Package.PackageID
	t.Result.Objects = uploadRet.Objects

	complete(nil, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
