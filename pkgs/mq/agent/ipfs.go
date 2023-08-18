package agent

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

type IPFSService interface {
	CheckIPFS(msg *CheckIPFS) (*CheckIPFSResp, *mq.CodeMessage)
}

// 检查节点上的IPFS
var _ = Register(IPFSService.CheckIPFS)

const (
	CHECK_IPFS_RESP_OP_DELETE_TEMP = "DeleteTemp"
	CHECK_IPFS_RESP_OP_CREATE_TEMP = "CreateTemp"
)

type CheckIPFS struct {
	IsComplete bool          `json:"isComplete"`
	Caches     []model.Cache `json:"caches"`
}
type CheckIPFSResp struct {
	Entries []CheckIPFSRespEntry `json:"entries"`
}
type CheckIPFSRespEntry struct {
	FileHash  string `json:"fileHash"`
	Operation string `json:"operation"`
}

func NewCheckIPFS(isComplete bool, caches []model.Cache) CheckIPFS {
	return CheckIPFS{
		IsComplete: isComplete,
		Caches:     caches,
	}
}
func NewCheckIPFSResp(entries []CheckIPFSRespEntry) CheckIPFSResp {
	return CheckIPFSResp{
		Entries: entries,
	}
}
func NewCheckIPFSRespEntry(fileHash string, op string) CheckIPFSRespEntry {
	return CheckIPFSRespEntry{
		FileHash:  fileHash,
		Operation: op,
	}
}
func (client *Client) CheckIPFS(msg CheckIPFS, opts ...mq.RequestOption) (*CheckIPFSResp, error) {
	return mq.Request[CheckIPFSResp](client.rabbitCli, msg, opts...)
}
