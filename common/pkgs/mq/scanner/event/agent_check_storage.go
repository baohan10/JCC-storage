package event

type AgentCheckStorage struct {
	EventBase
	StorageID  int64   `json:"storageID"`
	PackageIDs []int64 `json:"packageIDs"` // 需要检查的Package文件列表，如果为nil（不是为空），则代表进行全量检查
}

func NewAgentCheckStorage(storageID int64, packageIDs []int64) *AgentCheckStorage {
	return &AgentCheckStorage{
		StorageID:  storageID,
		PackageIDs: packageIDs,
	}
}

func init() {
	Register[*AgentCheckStorage]()
}
