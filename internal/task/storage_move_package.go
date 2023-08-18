package task

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/storage-client/internal/config"
	agtmq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
)

type StorageMovePackage struct {
	userID    int64
	packageID int64
	storageID int64
}

func NewStorageMovePackage(userID int64, packageID int64, storageID int64) *StorageMovePackage {
	return &StorageMovePackage{
		userID:    userID,
		packageID: packageID,
		storageID: storageID,
	}
}

func (t *StorageMovePackage) Execute(ctx TaskContext, complete CompleteFn) {
	err := t.do(ctx)
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *StorageMovePackage) do(ctx TaskContext) error {
	/*
		TODO2
		mutex, err := reqbuilder.NewBuilder().
			Metadata().
			// 用于判断用户是否有Storage权限
			UserStorage().ReadOne(t.packageID, t.storageID).
			// 用于判断用户是否有对象权限
			UserBucket().ReadAny().
			// 用于读取对象信息
			Object().ReadOne(t.packageID).
			// 用于查询Rep配置
			ObjectRep().ReadOne(t.packageID).
			// 用于查询Block配置
			ObjectBlock().ReadAny().
			// 用于创建Move记录
			StorageObject().CreateOne(t.storageID, t.userID, t.packageID).
			Storage().
			// 用于创建对象文件
			CreateOneObject(t.storageID, t.userID, t.packageID).
			MutexLock(ctx.DistLock)
		if err != nil {
			return fmt.Errorf("acquire locks failed, err: %w", err)
		}
		defer mutex.Unlock()
	*/
	getStgResp, err := ctx.coordinator.GetStorageInfo(coormq.NewGetStorageInfo(t.userID, t.storageID))
	if err != nil {
		return fmt.Errorf("getting storage info: %w", err)
	}

	// 然后向代理端发送移动文件的请求
	agentClient, err := agtmq.NewClient(getStgResp.NodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		return fmt.Errorf("create agent client to %d failed, err: %w", getStgResp.NodeID, err)
	}
	defer agentClient.Close()

	agentMoveResp, err := agentClient.StartStorageMovePackage(
		agtmq.NewStartStorageMovePackage(
			t.userID,
			t.packageID,
			t.storageID,
		))
	if err != nil {
		return fmt.Errorf("start moving package to storage: %w", err)
	}

	for {
		waitResp, err := agentClient.WaitStorageMovePackage(agtmq.NewWaitStorageMovePackage(agentMoveResp.TaskID, int64(time.Second)*5))
		if err != nil {
			return fmt.Errorf("wait moving package: %w", err)
		}

		if waitResp.IsComplete {
			if waitResp.Error != "" {
				return fmt.Errorf("agent moving package: %s", waitResp.Error)
			}

			break
		}
	}

	_, err = ctx.Coordinator().PackageMovedToStorage(coormq.NewPackageMovedToStorage(t.userID, t.packageID, t.storageID))
	if err != nil {
		return fmt.Errorf("moving package to storage: %w", err)
	}
	return nil
}
