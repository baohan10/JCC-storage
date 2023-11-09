package event

type AgentCheckCache struct {
	EventBase
	NodeID     int64    `json:"nodeID"`
	FileHashes []string `json:"fileHashes"` // 需要检查的FileHash列表，如果为nil（不是为空），则代表进行全量检查
}

func NewAgentCheckCache(nodeID int64, fileHashes []string) *AgentCheckCache {
	return &AgentCheckCache{
		NodeID:     nodeID,
		FileHashes: fileHashes,
	}
}

func init() {
	Register[*AgentCheckCache]()
}
