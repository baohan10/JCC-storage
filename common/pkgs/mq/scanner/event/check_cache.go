package event

type CheckCache struct {
	EventBase
	NodeID int64 `json:"nodeID"`
}

func NewCheckCache(nodeID int64) *CheckCache {
	return &CheckCache{
		NodeID: nodeID,
	}
}

func init() {
	Register[*CheckCache]()
}
