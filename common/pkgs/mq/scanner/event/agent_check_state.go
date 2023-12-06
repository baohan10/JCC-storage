package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type AgentCheckState struct {
	EventBase
	NodeID cdssdk.NodeID `json:"nodeID"`
}

func NewAgentCheckState(nodeID cdssdk.NodeID) *AgentCheckState {
	return &AgentCheckState{
		NodeID: nodeID,
	}
}

func init() {
	Register[*AgentCheckState]()
}
