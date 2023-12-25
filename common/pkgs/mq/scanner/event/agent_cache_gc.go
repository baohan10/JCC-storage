package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type AgentCacheGC struct {
	EventBase
	NodeID cdssdk.NodeID `json:"nodeID"`
}

func NewAgentCacheGC(nodeID cdssdk.NodeID) *AgentCacheGC {
	return &AgentCacheGC{
		NodeID: nodeID,
	}
}

func init() {
	Register[*AgentCacheGC]()
}
