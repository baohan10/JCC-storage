package event

type CheckRepCount struct {
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
