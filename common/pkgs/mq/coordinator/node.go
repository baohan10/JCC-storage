package coordinator

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type NodeService interface {
	GetUserNodes(msg *GetUserNodes) (*GetUserNodesResp, *mq.CodeMessage)

	GetNodes(msg *GetNodes) (*GetNodesResp, *mq.CodeMessage)

	GetNodeConnectivities(msg *GetNodeConnectivities) (*GetNodeConnectivitiesResp, *mq.CodeMessage)

	UpdateNodeConnectivities(msg *UpdateNodeConnectivities) (*UpdateNodeConnectivitiesResp, *mq.CodeMessage)
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

// 获取节点连通性信息
var _ = Register(Service.GetNodeConnectivities)

type GetNodeConnectivities struct {
	mq.MessageBodyBase
	NodeIDs []cdssdk.NodeID `json:"nodeIDs"`
}
type GetNodeConnectivitiesResp struct {
	mq.MessageBodyBase
	Connectivities []cdssdk.NodeConnectivity `json:"nodes"`
}

func ReqGetNodeConnectivities(nodeIDs []cdssdk.NodeID) *GetNodeConnectivities {
	return &GetNodeConnectivities{
		NodeIDs: nodeIDs,
	}
}
func RespGetNodeConnectivities(cons []cdssdk.NodeConnectivity) *GetNodeConnectivitiesResp {
	return &GetNodeConnectivitiesResp{
		Connectivities: cons,
	}
}
func (client *Client) GetNodeConnectivities(msg *GetNodeConnectivities) (*GetNodeConnectivitiesResp, error) {
	return mq.Request(Service.GetNodeConnectivities, client.rabbitCli, msg)
}

// 批量更新节点连通性信息
var _ = Register(Service.UpdateNodeConnectivities)

type UpdateNodeConnectivities struct {
	mq.MessageBodyBase
	Connectivities []cdssdk.NodeConnectivity `json:"connectivities"`
}
type UpdateNodeConnectivitiesResp struct {
	mq.MessageBodyBase
}

func ReqUpdateNodeConnectivities(cons []cdssdk.NodeConnectivity) *UpdateNodeConnectivities {
	return &UpdateNodeConnectivities{
		Connectivities: cons,
	}
}
func RespUpdateNodeConnectivities() *UpdateNodeConnectivitiesResp {
	return &UpdateNodeConnectivitiesResp{}
}
func (client *Client) UpdateNodeConnectivities(msg *UpdateNodeConnectivities) (*UpdateNodeConnectivitiesResp, error) {
	return mq.Request(Service.UpdateNodeConnectivities, client.rabbitCli, msg)
}
