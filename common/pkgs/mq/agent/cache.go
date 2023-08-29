package agent

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type CacheService interface {
	CheckCache(msg *CheckCache) (*CheckCacheResp, *mq.CodeMessage)

	StartCacheMovePackage(msg *StartCacheMovePackage) (*StartCacheMovePackageResp, *mq.CodeMessage)
	WaitCacheMovePackage(msg *WaitCacheMovePackage) (*WaitCacheMovePackageResp, *mq.CodeMessage)
}

// 检查节点上的IPFS
var _ = Register(CacheService.CheckCache)

const (
	CHECK_IPFS_RESP_OP_DELETE_TEMP = "DeleteTemp"
	CHECK_IPFS_RESP_OP_CREATE_TEMP = "CreateTemp"
)

type CheckCache struct {
	IsComplete bool          `json:"isComplete"`
	Caches     []model.Cache `json:"caches"`
}
type CheckCacheResp struct {
	Entries []CheckIPFSRespEntry `json:"entries"`
}
type CheckIPFSRespEntry struct {
	FileHash  string `json:"fileHash"`
	Operation string `json:"operation"`
}

func NewCheckCache(isComplete bool, caches []model.Cache) CheckCache {
	return CheckCache{
		IsComplete: isComplete,
		Caches:     caches,
	}
}
func NewCheckCacheResp(entries []CheckIPFSRespEntry) CheckCacheResp {
	return CheckCacheResp{
		Entries: entries,
	}
}
func NewCheckCacheRespEntry(fileHash string, op string) CheckIPFSRespEntry {
	return CheckIPFSRespEntry{
		FileHash:  fileHash,
		Operation: op,
	}
}
func (client *Client) CheckCache(msg CheckCache, opts ...mq.RequestOption) (*CheckCacheResp, error) {
	return mq.Request[CheckCacheResp](client.rabbitCli, msg, opts...)
}

// 将Package的缓存移动到这个节点
var _ = Register(CacheService.StartCacheMovePackage)

type StartCacheMovePackage struct {
	UserID    int64 `json:"userID"`
	PackageID int64 `json:"packageID"`
}
type StartCacheMovePackageResp struct {
	TaskID string `json:"taskID"`
}

func NewStartCacheMovePackage(userID int64, packageID int64) StartCacheMovePackage {
	return StartCacheMovePackage{
		UserID:    userID,
		PackageID: packageID,
	}
}
func NewStartCacheMovePackageResp(taskID string) StartCacheMovePackageResp {
	return StartCacheMovePackageResp{
		TaskID: taskID,
	}
}
func (client *Client) StartCacheMovePackage(msg StartCacheMovePackage, opts ...mq.RequestOption) (*StartCacheMovePackageResp, error) {
	return mq.Request[StartCacheMovePackageResp](client.rabbitCli, msg, opts...)
}

// 将Package的缓存移动到这个节点
var _ = Register(CacheService.WaitCacheMovePackage)

type WaitCacheMovePackage struct {
	TaskID        string `json:"taskID"`
	WaitTimeoutMs int64  `json:"waitTimeout"`
}
type WaitCacheMovePackageResp struct {
	IsComplete bool   `json:"isComplete"`
	Error      string `json:"error"`
}

func NewWaitCacheMovePackage(taskID string, waitTimeoutMs int64) WaitCacheMovePackage {
	return WaitCacheMovePackage{
		TaskID:        taskID,
		WaitTimeoutMs: waitTimeoutMs,
	}
}
func NewWaitCacheMovePackageResp(isComplete bool, err string) WaitCacheMovePackageResp {
	return WaitCacheMovePackageResp{
		IsComplete: isComplete,
		Error:      err,
	}
}
func (client *Client) WaitCacheMovePackage(msg WaitCacheMovePackage, opts ...mq.RequestOption) (*WaitCacheMovePackageResp, error) {
	return mq.Request[WaitCacheMovePackageResp](client.rabbitCli, msg, opts...)
}
