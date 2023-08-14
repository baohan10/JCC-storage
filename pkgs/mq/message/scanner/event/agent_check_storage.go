package event

type AgentCheckStorage struct {
	StorageID int64   `json:"storageID"`
	ObjectIDs []int64 `json:"objectIDs"` // 需要检查的Object文件列表，如果为nil（不是为空），则代表进行全量检查
}

func NewAgentCheckStorage(storageID int64, objectIDs []int64) AgentCheckStorage {
	return AgentCheckStorage{
		StorageID: storageID,
		ObjectIDs: objectIDs,
	}
}

func init() {
	Register[AgentCheckStorage]()
}
