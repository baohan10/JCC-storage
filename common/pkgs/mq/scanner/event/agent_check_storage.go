package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type AgentCheckStorage struct {
	EventBase
	StorageID  cdssdk.StorageID   `json:"storageID"`
	PackageIDs []cdssdk.PackageID `json:"packageIDs"` // 需要检查的Package文件列表，如果为nil（不是为空），则代表进行全量检查
}

func NewAgentCheckStorage(storageID cdssdk.StorageID, packageIDs []cdssdk.PackageID) *AgentCheckStorage {
	return &AgentCheckStorage{
		StorageID:  storageID,
		PackageIDs: packageIDs,
	}
}

func init() {
	Register[*AgentCheckStorage]()
}
