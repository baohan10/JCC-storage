package task

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/client/internal/config"
	"gitlink.org.cn/cloudream/common/pkg/distlock/reqbuilder"

	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
)

type MoveObjectToStorage struct {
	userID    int64
	objectID  int64
	storageID int64
}

type MoveObjectDirToStorage struct {
	userID                 int64
	dirName                string
	storageID              int64
	ResultObjectToStorages []ResultObjectToStorage
}

type ResultObjectToStorage struct {
	ObjectName string
	Error      error
}

func NewMoveObjectToStorage(userID int64, objectID int64, storageID int64) *MoveObjectToStorage {
	return &MoveObjectToStorage{
		userID:    userID,
		objectID:  objectID,
		storageID: storageID,
	}
}

func NewMoveObjectDirToStorage(userID int64, dirName string, storageID int64) *MoveObjectDirToStorage {
	return &MoveObjectDirToStorage{
		userID:    userID,
		dirName:   dirName,
		storageID: storageID,
	}
}

func (t *MoveObjectToStorage) Execute(ctx TaskContext, complete CompleteFn) {
	err := t.do(ctx)
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *MoveObjectDirToStorage) Execute(ctx TaskContext, complete CompleteFn) {
	err := t.do(ctx)
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *MoveObjectToStorage) do(ctx TaskContext) error {
	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 用于判断用户是否有Storage权限
		UserStorage().ReadOne(t.objectID, t.storageID).
		// 用于判断用户是否有对象权限
		UserBucket().ReadAny().
		// 用于读取对象信息
		Object().ReadOne(t.objectID).
		// 用于查询Rep配置
		ObjectRep().ReadOne(t.objectID).
		// 用于查询Block配置
		ObjectBlock().ReadAny().
		// 用于创建Move记录
		StorageObject().CreateOne(t.storageID, t.userID, t.objectID).
		Storage().
		// 用于创建对象文件
		CreateOneObject(t.storageID, t.userID, t.objectID).
		MutexLock(ctx.DistLock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	err = moveSingleObjectToStorage(ctx, t.userID, t.objectID, t.storageID)
	return err
}

func (t *MoveObjectDirToStorage) do(ctx TaskContext) error {
	//根据dirName查询相关的所有文件
	objsResp, err := ctx.Coordinator.GetObjectsByDirName(coormsg.NewGetObjectsByDirName(t.userID, t.dirName))
	if err != nil {
		return fmt.Errorf("get objectID by dirName failed: %w", err)
	}
	if len(objsResp.Objects) == 0 {
		return fmt.Errorf("dirName %v is not exist", t.dirName)
	}

	reqBlder := reqbuilder.NewBuilder()
	for _, object := range objsResp.Objects {
		reqBlder.Metadata().
			// 用于判断用户是否有Storage权限
			UserStorage().ReadOne(object.ObjectID, t.storageID).
			// 用于读取对象信息
			Object().ReadOne(object.ObjectID).
			// 用于查询Rep配置
			ObjectRep().ReadOne(object.ObjectID).
			// 用于创建Move记录
			StorageObject().CreateOne(t.storageID, t.userID, object.ObjectID).
			// 用于创建对象文件
			Storage().CreateOneObject(t.storageID, t.userID, object.ObjectID)
	}
	mutex, err := reqBlder.
		Metadata().
		// 用于判断用户是否有对象权限
		UserBucket().ReadAny().
		// 用于查询Block配置
		ObjectBlock().ReadAny().
		MutexLock(ctx.DistLock)

	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	for i := 0; i < len(objsResp.Objects); i++ {
		err := moveSingleObjectToStorage(ctx, t.userID, objsResp.Objects[i].ObjectID, t.storageID)
		t.ResultObjectToStorages = append(t.ResultObjectToStorages, ResultObjectToStorage{
			ObjectName: objsResp.Objects[i].Name,
			Error:      err,
		})
	}
	return nil
}

func moveSingleObjectToStorage(ctx TaskContext, userID int64, objectID int64, storageID int64) error {

	// 先向协调端请求文件相关的元数据
	preMoveResp, err := ctx.Coordinator.PreMoveObjectToStorage(coormsg.NewPreMoveObjectToStorage(objectID, storageID, userID))
	if err != nil {
		return fmt.Errorf("pre move object to storage: %w", err)
	}

	// 然后向代理端发送移动文件的请求
	agentClient, err := agtcli.NewClient(preMoveResp.NodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		return fmt.Errorf("create agent client to %d failed, err: %w", preMoveResp.NodeID, err)
	}
	defer agentClient.Close()

	agentMoveResp, err := agentClient.StartMovingObjectToStorage(
		agtmsg.NewStartMovingObjectToStorage(preMoveResp.Directory,
			objectID,
			userID,
			preMoveResp.FileSize,
			preMoveResp.Redundancy,
			preMoveResp.RedundancyData,
		))
	if err != nil {
		return fmt.Errorf("start moving object to storage: %w", err)
	}

	for {
		waitResp, err := agentClient.WaitMovingObject(agtmsg.NewWaitMovingObject(agentMoveResp.TaskID, int64(time.Second)*5))
		if err != nil {
			return fmt.Errorf("wait moving object: %w", err)
		}

		if waitResp.IsComplete {
			if waitResp.Error != "" {
				return fmt.Errorf("agent moving object: %s", waitResp.Error)
			}

			break
		}
	}

	_, err = ctx.Coordinator.MoveObjectToStorage(coormsg.NewMoveObjectToStorage(objectID, storageID, userID))
	if err != nil {
		return fmt.Errorf("moving object to storage: %w", err)
	}
	return nil

}
