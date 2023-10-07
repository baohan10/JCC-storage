package event

var _ = Register[*CheckRepCount]()

type CheckRepCount struct {
	EventBase
	FileHashes []string `json:"fileHashes"`
}

func NewCheckRepCount(fileHashes []string) *CheckRepCount {
	return &CheckRepCount{
		FileHashes: fileHashes,
	}
}
