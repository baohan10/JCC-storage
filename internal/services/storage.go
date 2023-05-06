package services

import (
	"fmt"

	"gitlink.org.cn/cloudream/client/internal/config"
	agtcli "gitlink.org.cn/cloudream/rabbitmq/client/agent"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
	"gitlink.org.cn/cloudream/utils/consts"
	"gitlink.org.cn/cloudream/utils/consts/errorcode"
)

type StorageService struct {
	*Service
}

func StorageSvc(svc *Service) *StorageService {
	return &StorageService{Service: svc}
}

func (svc *StorageService) MoveObjectToStorage(userID int, objectID int, storageID int) error {
	// 先向协调端请求文件相关的元数据
	moveResp, err := svc.coordinator.Move(coormsg.NewMoveObjectToStorageBody(objectID, storageID, userID))
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if moveResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator operation failed, code: %s, message: %s", moveResp.ErrorCode, moveResp.ErrorMessage)
	}

	// 然后向代理端发送移动文件的请求
	agentClient, err := agtcli.NewAgentClient(moveResp.Body.NodeID, &config.Cfg().RabbitMQ)
	if err != nil {
		return fmt.Errorf("create agent client to %d failed, err: %w", moveResp.Body.NodeID, err)
	}
	defer agentClient.Close()

	switch moveResp.Body.Redundancy {
	case consts.REDUNDANCY_REP:
		agentMoveResp, err := agentClient.RepMove(agtmsg.NewRepMoveCommandBody(moveResp.Body.Directory, moveResp.Body.Hashes, objectID, userID, moveResp.Body.FileSizeInBytes))
		if err != nil {
			return fmt.Errorf("request to agent %d failed, err: %w", moveResp.Body.NodeID, err)
		}
		if agentMoveResp.ErrorCode != errorcode.OK {
			return fmt.Errorf("agent %d operation failed, code: %s, messsage: %s", moveResp.Body.NodeID, agentMoveResp.ErrorCode, agentMoveResp.ErrorMessage)
		}

	case consts.REDUNDANCY_EC:
		agentMoveResp, err := agentClient.ECMove(agtmsg.NewECMoveCommandBody(moveResp.Body.Directory, moveResp.Body.Hashes, moveResp.Body.IDs, *moveResp.Body.ECName, objectID, userID, moveResp.Body.FileSizeInBytes))
		if err != nil {
			return fmt.Errorf("request to agent %d failed, err: %w", moveResp.Body.NodeID, err)
		}
		if agentMoveResp.ErrorCode != errorcode.OK {
			return fmt.Errorf("agent %d operation failed, code: %s, messsage: %s", moveResp.Body.NodeID, agentMoveResp.ErrorCode, agentMoveResp.ErrorMessage)
		}
	}

	return nil
}

func (svc *StorageService) DeleteStorageObject(userID int, objectID int, storageID int) error {
	// TODO
	panic("not implement yet")
}
