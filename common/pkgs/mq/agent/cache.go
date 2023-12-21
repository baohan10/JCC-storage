package agent

import (
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

type CacheService interface {
	CheckCache(msg *CheckCache) (*CheckCacheResp, *mq.CodeMessage)

	CacheGC(msg *CacheGC) (*CacheGCResp, *mq.CodeMessage)

	StartCacheMovePackage(msg *StartCacheMovePackage) (*StartCacheMovePackageResp, *mq.CodeMessage)
	WaitCacheMovePackage(msg *WaitCacheMovePackage) (*WaitCacheMovePackageResp, *mq.CodeMessage)
}

// 检查节点上的IPFS
var _ = Register(Service.CheckCache)

type CheckCache struct {
	mq.MessageBodyBase
}
type CheckCacheResp struct {
	mq.MessageBodyBase
	FileHashes []string `json:"fileHashes"`
}

func NewCheckCache() *CheckCache {
	return &CheckCache{}
}
func NewCheckCacheResp(fileHashes []string) *CheckCacheResp {
	return &CheckCacheResp{
		FileHashes: fileHashes,
	}
}
func (client *Client) CheckCache(msg *CheckCache, opts ...mq.RequestOption) (*CheckCacheResp, error) {
	return mq.Request(Service.CheckCache, client.rabbitCli, msg, opts...)
}

// 清理Cache中不用的文件
var _ = Register(Service.CacheGC)

type CacheGC struct {
	mq.MessageBodyBase
	PinnedFileHashes []string `json:"pinnedFileHashes"`
}
type CacheGCResp struct {
	mq.MessageBodyBase
}

func ReqCacheGC(pinnedFileHashes []string) *CacheGC {
	return &CacheGC{
		PinnedFileHashes: pinnedFileHashes,
	}
}
func RespCacheGC() *CacheGCResp {
	return &CacheGCResp{}
}
func (client *Client) CacheGC(msg *CacheGC, opts ...mq.RequestOption) (*CacheGCResp, error) {
	return mq.Request(Service.CacheGC, client.rabbitCli, msg, opts...)
}

// 将Package的缓存移动到这个节点
var _ = Register(Service.StartCacheMovePackage)

type StartCacheMovePackage struct {
	mq.MessageBodyBase
	UserID    cdssdk.UserID    `json:"userID"`
	PackageID cdssdk.PackageID `json:"packageID"`
}
type StartCacheMovePackageResp struct {
	mq.MessageBodyBase
	TaskID string `json:"taskID"`
}

func NewStartCacheMovePackage(userID cdssdk.UserID, packageID cdssdk.PackageID) *StartCacheMovePackage {
	return &StartCacheMovePackage{
		UserID:    userID,
		PackageID: packageID,
	}
}
func NewStartCacheMovePackageResp(taskID string) *StartCacheMovePackageResp {
	return &StartCacheMovePackageResp{
		TaskID: taskID,
	}
}
func (client *Client) StartCacheMovePackage(msg *StartCacheMovePackage, opts ...mq.RequestOption) (*StartCacheMovePackageResp, error) {
	return mq.Request(Service.StartCacheMovePackage, client.rabbitCli, msg, opts...)
}

// 将Package的缓存移动到这个节点
var _ = Register(Service.WaitCacheMovePackage)

type WaitCacheMovePackage struct {
	mq.MessageBodyBase
	TaskID        string `json:"taskID"`
	WaitTimeoutMs int64  `json:"waitTimeout"`
}
type WaitCacheMovePackageResp struct {
	mq.MessageBodyBase
	IsComplete bool   `json:"isComplete"`
	Error      string `json:"error"`
}

func NewWaitCacheMovePackage(taskID string, waitTimeoutMs int64) *WaitCacheMovePackage {
	return &WaitCacheMovePackage{
		TaskID:        taskID,
		WaitTimeoutMs: waitTimeoutMs,
	}
}
func NewWaitCacheMovePackageResp(isComplete bool, err string) *WaitCacheMovePackageResp {
	return &WaitCacheMovePackageResp{
		IsComplete: isComplete,
		Error:      err,
	}
}
func (client *Client) WaitCacheMovePackage(msg *WaitCacheMovePackage, opts ...mq.RequestOption) (*WaitCacheMovePackageResp, error) {
	return mq.Request(Service.WaitCacheMovePackage, client.rabbitCli, msg, opts...)
}
