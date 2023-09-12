package event

type AgentCheckState struct {
	NodeID int64 `json:"nodeID"`
}

func NewAgentCheckState(nodeID int64) *AgentCheckState {
	return &AgentCheckState{
		NodeID: nodeID,
	}
}

func init() {
	Register[AgentCheckState]()
}
