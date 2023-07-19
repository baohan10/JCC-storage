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
	userID    int
	objectID  int
	storageID int
}

func NewMoveObjectToStorage(userID int, objectID int, storageID int) *MoveObjectToStorage {
	return &MoveObjectToStorage{
		userID:    userID,
		objectID:  objectID,
		storageID: storageID,
	}
}

func (t *MoveObjectToStorage) Execute(ctx TaskContext, complete CompleteFn) {
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

	// 先向协调端请求文件相关的元数据
	preMoveResp, err := ctx.Coordinator.PreMoveObjectToStorage(coormsg.NewPreMoveObjectToStorageBody(t.objectID, t.storageID, t.userID))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if preMoveResp.IsFailed() {
		return fmt.Errorf("coordinator PreMoveObjectToStorage failed, code: %s, message: %s", preMoveResp.ErrorCode, preMoveResp.ErrorMessage)
	}

	// 然后向代理端发送移动文件的请求
	agentClient, err := agtcli.NewClient(preMoveResp.Body.NodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		return fmt.Errorf("create agent client to %d failed, err: %w", preMoveResp.Body.NodeID, err)
	}
	defer agentClient.Close()

	agentMoveResp, err := agentClient.StartMovingObjectToStorage(
		agtmsg.NewStartMovingObjectToStorageBody(preMoveResp.Body.Directory,
			t.objectID,
			t.userID,
			preMoveResp.Body.FileSize,
			preMoveResp.Body.Redundancy,
			preMoveResp.Body.RedundancyData,
		))
	if err != nil {
		return fmt.Errorf("request to agent %d failed, err: %w", preMoveResp.Body.NodeID, err)
	}
	if agentMoveResp.IsFailed() {
		return fmt.Errorf("agent %d operation failed, code: %s, messsage: %s", preMoveResp.Body.NodeID, agentMoveResp.ErrorCode, agentMoveResp.ErrorMessage)
	}

	for {
		waitResp, err := agentClient.WaitMovingObject(agtmsg.NewWaitMovingObjectBody(agentMoveResp.Body.TaskID, int64(time.Second)*5))
		if err != nil {
			return fmt.Errorf("request to agent %d failed, err: %w", preMoveResp.Body.NodeID, err)
		}
		if preMoveResp.IsFailed() {
			return fmt.Errorf("coordinator WaitMovingObject failed, code: %s, message: %s", waitResp.ErrorCode, waitResp.ErrorMessage)
		}

		if waitResp.Body.IsComplete {
			if waitResp.Body.Error != "" {
				return fmt.Errorf("agent moving object: %s", waitResp.Body.Error)
			}

			break
		}
	}

	moveResp, err := ctx.Coordinator.MoveObjectToStorage(coormsg.NewMoveObjectToStorageBody(t.objectID, t.storageID, t.userID))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if preMoveResp.IsFailed() {
		return fmt.Errorf("coordinator MoveObjectToStorage failed, code: %s, message: %s", moveResp.ErrorCode, moveResp.ErrorMessage)
	}

	return nil
}
