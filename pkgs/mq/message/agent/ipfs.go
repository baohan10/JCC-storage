package agent

import (
	"gitlink.org.cn/cloudream/common/pkg/mq"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

type CheckIPFS struct {
	IsComplete bool          `json:"isComplete"`
	Caches     []model.Cache `json:"caches"`
}

func NewCheckIPFS(isComplete bool, caches []model.Cache) CheckIPFS {
	return CheckIPFS{
		IsComplete: isComplete,
		Caches:     caches,
	}
}

type CheckIPFSResp struct {
	Entries []CheckIPFSRespEntry `json:"entries"`
}

const (
	CHECK_IPFS_RESP_OP_DELETE_TEMP = "DeleteTemp"
	CHECK_IPFS_RESP_OP_CREATE_TEMP = "CreateTemp"
)

type CheckIPFSRespEntry struct {
	FileHash  string `json:"fileHash"`
	Operation string `json:"operation"`
}

func NewCheckIPFSRespEntry(fileHash string, op string) CheckIPFSRespEntry {
	return CheckIPFSRespEntry{
		FileHash:  fileHash,
		Operation: op,
	}
}

func NewCheckIPFSResp(entries []CheckIPFSRespEntry) CheckIPFSResp {
	return CheckIPFSResp{
		Entries: entries,
	}
}

func init() {
	mq.RegisterMessage[CheckIPFS]()
	mq.RegisterMessage[CheckIPFSResp]()
}
