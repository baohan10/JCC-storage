package event

type CheckRepCount struct {
	EventBase
	FileHashes []string `json:"fileHashes"`
}

func NewCheckRepCount(fileHashes []string) *CheckRepCount {
	return &CheckRepCount{
		FileHashes: fileHashes,
	}
}

func init() {
	Register[CheckRepCount]()
}
