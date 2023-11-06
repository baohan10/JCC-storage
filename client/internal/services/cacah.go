package services

import (
	"fmt"
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type CacheService struct {
	*Service
}

func (svc *Service) CacheSvc() *CacheService {
	return &CacheService{Service: svc}
}

func (svc *CacheService) StartCacheMovePackage(userID int64, packageID int64, nodeID int64) (string, error) {
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

func (svc *CacheService) WaitCacheMovePackage(nodeID int64, taskID string, waitTimeout time.Duration) (bool, []cdssdk.ObjectCacheInfo, error) {
	agentCli, err := stgglb.AgentMQPool.Acquire(nodeID)
	if err != nil {
		return true, nil, fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agentCli)

	waitResp, err := agentCli.WaitCacheMovePackage(agtmq.NewWaitCacheMovePackage(taskID, waitTimeout.Milliseconds()))
	if err != nil {
		return true, nil, fmt.Errorf("wait cache move package: %w", err)
	}

	if !waitResp.IsComplete {
		return false, nil, nil
	}

	if waitResp.Error != "" {
		return true, nil, fmt.Errorf("%s", waitResp.Error)
	}

	return true, waitResp.CacheInfos, nil
}

func (svc *CacheService) GetPackageObjectCacheInfos(userID int64, packageID int64) ([]cdssdk.ObjectCacheInfo, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getResp, err := coorCli.GetPackageObjectCacheInfos(coormq.NewGetPackageObjectCacheInfos(userID, packageID))
	if err != nil {
		return nil, fmt.Errorf("requesting to coodinator: %w", err)
	}

	return getResp.Infos, nil
}
