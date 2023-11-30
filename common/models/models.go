package stgmod

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

/// TODO 将分散在各处的公共结构体定义集中到这里来

type ObjectBlockData struct {
	Index         int             `json:"index"`
	FileHash      string          `json:"fileHash"`
	NodeID        cdssdk.NodeID   `json:"nodeID"`
	CachedNodeIDs []cdssdk.NodeID `json:"nodeIDs"`
}

func NewObjectBlockData(index int, fileHash string, nodeID cdssdk.NodeID, cachedNodeIDs []cdssdk.NodeID) ObjectBlockData {
	return ObjectBlockData{
		Index:         index,
		FileHash:      fileHash,
		CachedNodeIDs: cachedNodeIDs,
	}
}

type ObjectECData struct {
	Object model.Object      `json:"object"`
	Blocks []ObjectBlockData `json:"blocks"`
}

func NewObjectECData(object model.Object, blocks []ObjectBlockData) ObjectECData {
	return ObjectECData{
		Object: object,
		Blocks: blocks,
	}
}

type LocalMachineInfo struct {
	NodeID     *cdssdk.NodeID `json:"nodeID"`
	ExternalIP string         `json:"externalIP"`
	LocalIP    string         `json:"localIP"`
}
