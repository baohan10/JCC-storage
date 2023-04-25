package services

import (
	"fmt"

	racli "gitlink.org.cn/cloudream/rabbitmq/client"
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
	moveResp, err := svc.coordinator.Move(objectID, storageID, userID)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if moveResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator operation failed, code: %s, message: %s", moveResp.ErrorCode, moveResp.Message)
	}

	// 然后向代理端发送移动文件的请求
	agentClient, err := racli.NewAgentClient(moveResp.NodeID)
	if err != nil {
		return fmt.Errorf("create agent client to %d failed, err: %w", storageID, err)
	}
	defer agentClient.Close()

	switch moveResp.Redundancy {
	case consts.REDUNDANCY_REP:
		agentMoveResp, err := agentClient.RepMove(moveResp.Directory, moveResp.Hashes, objectID, userID, moveResp.FileSizeInBytes)
		if err != nil {
			return fmt.Errorf("request to agent %d failed, err: %w", storageID, err)
		}
		if agentMoveResp.ErrorCode != errorcode.OK {
			return fmt.Errorf("agent %d operation failed, code: %s, messsage: %s", storageID, agentMoveResp.ErrorCode, agentMoveResp.Message)
		}

	case consts.REDUNDANCY_EC:
		agentMoveResp, err := agentClient.ECMove(moveResp.Directory, moveResp.Hashes, moveResp.IDs, *moveResp.ECName, objectID, userID, moveResp.FileSizeInBytes)
		if err != nil {
			return fmt.Errorf("request to agent %d failed, err: %w", storageID, err)
		}
		if agentMoveResp.ErrorCode != errorcode.OK {
			return fmt.Errorf("agent %d operation failed, code: %s, messsage: %s", storageID, agentMoveResp.ErrorCode, agentMoveResp.Message)
		}
	}

	return nil
}

func (svc *StorageService) DeleteStorageObject(userID int, objectID int, storageID int) error {
	// TODO
	panic("not implement yet")
}
