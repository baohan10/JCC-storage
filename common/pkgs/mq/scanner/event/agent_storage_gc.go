package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type AgentStorageGC struct {
	EventBase
	StorageID cdssdk.StorageID `json:"storageID"`
}

func NewAgentStorageGC(storageID cdssdk.StorageID) *AgentStorageGC {
	return &AgentStorageGC{
		StorageID: storageID,
	}
}

func init() {
	Register[*AgentStorageGC]()
}
