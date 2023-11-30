package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type AgentCheckCache struct {
	EventBase
	NodeID     cdssdk.NodeID `json:"nodeID"`
	FileHashes []string      `json:"fileHashes"` // 需要检查的FileHash列表，如果为nil（不是为空），则代表进行全量检查
}

func NewAgentCheckCache(nodeID cdssdk.NodeID, fileHashes []string) *AgentCheckCache {
	return &AgentCheckCache{
		NodeID:     nodeID,
		FileHashes: fileHashes,
	}
}

func init() {
	Register[*AgentCheckCache]()
}
