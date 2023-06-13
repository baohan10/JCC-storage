package services

import (
	"fmt"

	"gitlink.org.cn/cloudream/client/internal/config"
	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
)

type StorageService struct {
	*Service
}

func (svc *Service) StorageSvc() *StorageService {
	return &StorageService{Service: svc}
}

func (svc *StorageService) MoveObjectToStorage(userID int, objectID int, storageID int) error {
	// 先向协调端请求文件相关的元数据
	preMoveResp, err := svc.coordinator.PreMoveObjectToStorage(coormsg.NewPreMoveObjectToStorageBody(objectID, storageID, userID))
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

	agentMoveResp, err := agentClient.MoveObjectToStorage(
		agtmsg.NewMoveObjectToStorageBody(preMoveResp.Body.Directory,
			objectID,
			userID,
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

	moveResp, err := svc.coordinator.MoveObjectToStorage(coormsg.NewMoveObjectToStorageBody(objectID, storageID, userID))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if preMoveResp.IsFailed() {
		return fmt.Errorf("coordinator MoveObjectToStorage failed, code: %s, message: %s", moveResp.ErrorCode, moveResp.ErrorMessage)
	}

	return nil
}

func (svc *StorageService) DeleteStorageObject(userID int, objectID int, storageID int) error {
	// TODO
	panic("not implement yet")
}
