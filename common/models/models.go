package stgmod

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type ObjectBlock struct {
	ObjectID cdssdk.ObjectID `db:"ObjectID" json:"objectID"`
	Index    int             `db:"Index" json:"index"`
	NodeID   cdssdk.NodeID   `db:"NodeID" json:"nodeID"` // 这个块应该在哪个节点上
	FileHash string          `db:"FileHash" json:"fileHash"`
}

type ObjectBlockDetail struct {
	ObjectID      cdssdk.ObjectID `json:"objectID"`
	Index         int             `json:"index"`
	FileHash      string          `json:"fileHash"`
	NodeIDs       []cdssdk.NodeID `json:"nodeID"`        // 这个块应该在哪些节点上
	CachedNodeIDs []cdssdk.NodeID `json:"cachedNodeIDs"` // 哪些节点实际缓存了这个块
}

func NewObjectBlockDetail(objID cdssdk.ObjectID, index int, fileHash string, nodeIDs []cdssdk.NodeID, cachedNodeIDs []cdssdk.NodeID) ObjectBlockDetail {
	return ObjectBlockDetail{
		ObjectID:      objID,
		Index:         index,
		FileHash:      fileHash,
		NodeIDs:       nodeIDs,
		CachedNodeIDs: cachedNodeIDs,
	}
}

type ObjectDetail struct {
	Object cdssdk.Object       `json:"object"`
	Blocks []ObjectBlockDetail `json:"blocks"`
}

func NewObjectDetail(object cdssdk.Object, blocks []ObjectBlockDetail) ObjectDetail {
	return ObjectDetail{
		Object: object,
		Blocks: blocks,
	}
}

type LocalMachineInfo struct {
	NodeID     *cdssdk.NodeID    `json:"nodeID"`
	ExternalIP string            `json:"externalIP"`
	LocalIP    string            `json:"localIP"`
	LocationID cdssdk.LocationID `json:"locationID"`
}
