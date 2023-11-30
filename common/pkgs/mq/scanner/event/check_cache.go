package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type CheckCache struct {
	EventBase
	NodeID cdssdk.NodeID `json:"nodeID"`
}

func NewCheckCache(nodeID cdssdk.NodeID) *CheckCache {
	return &CheckCache{
		NodeID: nodeID,
	}
}

func init() {
	Register[*CheckCache]()
}
