package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type AgentCheckCache struct {
	EventBase
	NodeID cdssdk.NodeID `json:"nodeID"`
}

func NewAgentCheckCache(nodeID cdssdk.NodeID) *AgentCheckCache {
	return &AgentCheckCache{
		NodeID: nodeID,
	}
}

func init() {
	Register[*AgentCheckCache]()
}
