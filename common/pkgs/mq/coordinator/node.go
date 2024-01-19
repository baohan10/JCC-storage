package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type NodeService interface {
	GetUserNodes(msg *GetUserNodes) (*GetUserNodesResp, *mq.CodeMessage)

	GetNodes(msg *GetNodes) (*GetNodesResp, *mq.CodeMessage)
}

// 查询用户可用的节点
var _ = Register(Service.GetUserNodes)

type GetUserNodes struct {
	mq.MessageBodyBase
	UserID cdssdk.UserID `json:"userID"`
}
type GetUserNodesResp struct {
	mq.MessageBodyBase
	Nodes []cdssdk.Node `json:"nodes"`
}

func NewGetUserNodes(userID cdssdk.UserID) *GetUserNodes {
	return &GetUserNodes{
		UserID: userID,
	}
}
func NewGetUserNodesResp(nodes []cdssdk.Node) *GetUserNodesResp {
	return &GetUserNodesResp{
		Nodes: nodes,
	}
}
func (client *Client) GetUserNodes(msg *GetUserNodes) (*GetUserNodesResp, error) {
	return mq.Request(Service.GetUserNodes, client.rabbitCli, msg)
}

// 获取指定节点的信息。如果NodeIDs为nil，则返回所有Node
var _ = Register(Service.GetNodes)

type GetNodes struct {
	mq.MessageBodyBase
	NodeIDs []cdssdk.NodeID `json:"nodeIDs"`
}
type GetNodesResp struct {
	mq.MessageBodyBase
	Nodes []cdssdk.Node `json:"nodes"`
}

func NewGetNodes(nodeIDs []cdssdk.NodeID) *GetNodes {
	return &GetNodes{
		NodeIDs: nodeIDs,
	}
}
func NewGetNodesResp(nodes []cdssdk.Node) *GetNodesResp {
	return &GetNodesResp{
		Nodes: nodes,
	}
}
func (r *GetNodesResp) GetNode(id cdssdk.NodeID) *cdssdk.Node {
	for _, n := range r.Nodes {
		if n.NodeID == id {
			return &n
		}
	}

	return nil
}
func (client *Client) GetNodes(msg *GetNodes) (*GetNodesResp, error) {
	return mq.Request(Service.GetNodes, client.rabbitCli, msg)
}
