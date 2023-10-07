package event

var _ = Register[*AgentCheckState]()

type AgentCheckState struct {
	EventBase
	NodeID int64 `json:"nodeID"`
}

func NewAgentCheckState(nodeID int64) *AgentCheckState {
	return &AgentCheckState{
		NodeID: nodeID,
	}
}
