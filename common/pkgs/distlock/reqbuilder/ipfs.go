package reqbuilder

import (
	"strconv"

	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/lockprovider"
)

type IPFSLockReqBuilder struct {
	*LockRequestBuilder
}

func (b *LockRequestBuilder) IPFS() *IPFSLockReqBuilder {
	return &IPFSLockReqBuilder{LockRequestBuilder: b}
}
func (b *IPFSLockReqBuilder) Buzy(nodeID cdssdk.NodeID) *IPFSLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(nodeID),
		Name:   lockprovider.IPFSBuzyLock,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}

func (b *IPFSLockReqBuilder) GC(nodeID cdssdk.NodeID) *IPFSLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(nodeID),
		Name:   lockprovider.IPFSGCLock,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}

func (b *IPFSLockReqBuilder) makePath(nodeID cdssdk.NodeID) []string {
	return []string{lockprovider.IPFSLockPathPrefix, strconv.FormatInt(int64(nodeID), 10)}
}
