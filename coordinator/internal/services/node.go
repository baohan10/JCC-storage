package services

import (
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
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
	var nodes []model.Node

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
