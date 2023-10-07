package event

var _ = Register[*CheckCache]()

type CheckCache struct {
	EventBase
	NodeID int64 `json:"nodeID"`
}

func NewCheckCache(nodeID int64) *CheckCache {
	return &CheckCache{
		NodeID: nodeID,
	}
}
