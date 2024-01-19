package services

import (
	"fmt"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type NodeService struct {
	*Service
}

func (svc *Service) NodeSvc() *NodeService {
	return &NodeService{Service: svc}
}

func (svc *NodeService) GetNodes(nodeIDs []cdssdk.NodeID) ([]cdssdk.Node, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getResp, err := coorCli.GetNodes(coormq.NewGetNodes(nodeIDs))
	if err != nil {
		return nil, fmt.Errorf("requsting to coodinator: %w", err)
	}

	return getResp.Nodes, nil
}
