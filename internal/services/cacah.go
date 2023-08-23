package services

import (
	"fmt"
	"time"

	"gitlink.org.cn/cloudream/storage-common/globals"
	agtmq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/agent"
)

type CacheService struct {
	*Service
}

func (svc *Service) CacheSvc() *CacheService {
	return &CacheService{Service: svc}
}

func (svc *CacheService) StartCacheMovePackage(userID int64, packageID int64, nodeID int64) (string, error) {
	agentCli, err := globals.AgentMQPool.Acquire(nodeID)
	if err != nil {
		return "", fmt.Errorf("new agent client: %w", err)
	}
	defer agentCli.Close()

	startResp, err := agentCli.StartCacheMovePackage(agtmq.NewStartCacheMovePackage(userID, packageID))
	if err != nil {
		return "", fmt.Errorf("start cache move package: %w", err)
	}

	return startResp.TaskID, nil
}

func (svc *CacheService) WaitCacheMovePackage(nodeID int64, taskID string, waitTimeout time.Duration) (bool, error) {
	agentCli, err := globals.AgentMQPool.Acquire(nodeID)
	if err != nil {
		return true, fmt.Errorf("new agent client: %w", err)
	}
	defer agentCli.Close()

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
