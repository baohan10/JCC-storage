package mq

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) GetUserNodes(msg *coormq.GetUserNodes) (*coormq.GetUserNodesResp, *mq.CodeMessage) {
	nodes, err := svc.db.Node().GetUserNodes(svc.db.SQLCtx(), msg.UserID)
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			Warnf("query user nodes failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "query user nodes failed")
	}

	return mq.ReplyOK(coormq.NewGetUserNodesResp(nodes))
}

func (svc *Service) GetNodes(msg *coormq.GetNodes) (*coormq.GetNodesResp, *mq.CodeMessage) {
	var nodes []cdssdk.Node

	if msg.NodeIDs == nil {
		var err error
		nodes, err = svc.db.Node().GetAllNodes(svc.db.SQLCtx())
		if err != nil {
			logger.Warnf("getting all nodes: %s", err.Error())
			return nil, mq.Failed(errorcode.OperationFailed, "get all node failed")
		}

	} else {
		// 可以不用事务
		for _, id := range msg.NodeIDs {
			node, err := svc.db.Node().GetByID(svc.db.SQLCtx(), id)
			if err != nil {
				logger.WithField("NodeID", id).
					Warnf("query node failed, err: %s", err.Error())
				return nil, mq.Failed(errorcode.OperationFailed, "query node failed")
			}

			nodes = append(nodes, node)
		}
	}

	return mq.ReplyOK(coormq.NewGetNodesResp(nodes))
}

func (svc *Service) GetNodeConnectivities(msg *coormq.GetNodeConnectivities) (*coormq.GetNodeConnectivitiesResp, *mq.CodeMessage) {
	cons, err := svc.db.NodeConnectivity().BatchGetByFromNode(svc.db.SQLCtx(), msg.NodeIDs)
	if err != nil {
		logger.Warnf("batch get node connectivities by from node: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "batch get node connectivities by from node failed")
	}

	return mq.ReplyOK(coormq.RespGetNodeConnectivities(cons))
}

func (svc *Service) UpdateNodeConnectivities(msg *coormq.UpdateNodeConnectivities) (*coormq.UpdateNodeConnectivitiesResp, *mq.CodeMessage) {
	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		// 只有发起节点和目的节点都存在，才能插入这条记录到数据库
		allNodes, err := svc.db.Node().GetAllNodes(tx)
		if err != nil {
			return fmt.Errorf("getting all nodes: %w", err)
		}

		allNodeID := make(map[cdssdk.NodeID]bool)
		for _, node := range allNodes {
			allNodeID[node.NodeID] = true
		}

		var avaiCons []cdssdk.NodeConnectivity
		for _, con := range msg.Connectivities {
			if allNodeID[con.FromNodeID] && allNodeID[con.ToNodeID] {
				avaiCons = append(avaiCons, con)
			}
		}

		err = svc.db.NodeConnectivity().BatchUpdateOrCreate(tx, avaiCons)
		if err != nil {
			return fmt.Errorf("batch update or create node connectivities: %s", err)
		}

		return nil
	})
	if err != nil {
		logger.Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, err.Error())
	}

	return mq.ReplyOK(coormq.RespUpdateNodeConnectivities())
}
