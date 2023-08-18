package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

type NodeService interface {
	GetUserNodes(msg *GetUserNodes) (*GetUserNodesResp, *mq.CodeMessage)

	GetNodes(msg *GetNodes) (*GetNodesResp, *mq.CodeMessage)
}

// 查询用户可用的节点
var _ = Register(NodeService.GetUserNodes)

type GetUserNodes struct {
	UserID int64 `json:"userID"`
}
type GetUserNodesResp struct {
	Nodes []model.Node `json:"nodes"`
}

func NewGetUserNodes(userID int64) GetUserNodes {
	return GetUserNodes{
		UserID: userID,
	}
}
func NewGetUserNodesResp(nodes []model.Node) GetUserNodesResp {
	return GetUserNodesResp{
		Nodes: nodes,
	}
}
func (client *Client) GetUserNodes(msg GetUserNodes) (*GetUserNodesResp, error) {
	return mq.Request[GetUserNodesResp](client.rabbitCli, msg)
}

// 获取指定节点的信息
var _ = Register(NodeService.GetNodes)

type GetNodes struct {
	NodeIDs []int64 `json:"nodeIDs"`
}
type GetNodesResp struct {
	Nodes []model.Node `json:"nodes"`
}

func NewGetNodes(nodeIDs []int64) GetNodes {
	return GetNodes{
		NodeIDs: nodeIDs,
	}
}
func NewGetNodesResp(nodes []model.Node) GetNodesResp {
	return GetNodesResp{
		Nodes: nodes,
	}
}
func (client *Client) GetNodes(msg GetNodes) (*GetNodesResp, error) {
	return mq.Request[GetNodesResp](client.rabbitCli, msg)
}
