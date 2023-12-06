package services

import (
	"fmt"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
)

type CacheService struct {
	*Service
}

func (svc *Service) CacheSvc() *CacheService {
	return &CacheService{Service: svc}
}

func (svc *CacheService) StartCacheMovePackage(userID cdssdk.UserID, packageID cdssdk.PackageID, nodeID cdssdk.NodeID) (string, error) {
	agentCli, err := stgglb.AgentMQPool.Acquire(nodeID)
	if err != nil {
		return "", fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

	startResp, err := agentCli.StartCacheMovePackage(agtmq.NewStartCacheMovePackage(userID, packageID))
	if err != nil {
		return "", fmt.Errorf("start cache move package: %w", err)
	}

	return startResp.TaskID, nil
}

func (svc *CacheService) WaitCacheMovePackage(nodeID cdssdk.NodeID, taskID string, waitTimeout time.Duration) (bool, error) {
	agentCli, err := stgglb.AgentMQPool.Acquire(nodeID)
	if err != nil {
		return true, fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

	waitResp, err := agentCli.WaitCacheMovePackage(agtmq.NewWaitCacheMovePackage(taskID, waitTimeout.Milliseconds()))
	if err != nil {
		return true, fmt.Errorf("wait cache move package: %w", err)
	}

	if !waitResp.IsComplete {
		return false, nil
	}

	if waitResp.Error != "" {
		return true, fmt.Errorf("%s", waitResp.Error)
	}

	return true, nil
}
