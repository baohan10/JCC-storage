package event

type CheckObject struct {
	ObjectIDs []int64 `json:"objectIDs"`
}

func NewCheckObject(objectIDs []int64) CheckObject {
	return CheckObject{
		ObjectIDs: objectIDs,
	}
}

func init() {
	Register[CheckObject]()
}
