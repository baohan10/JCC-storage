package reqbuilder

import (
	"strconv"

	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage-common/pkgs/distlock/lockprovider"
)

type IPFSLockReqBuilder struct {
	*LockRequestBuilder
}

func (b *LockRequestBuilder) IPFS() *IPFSLockReqBuilder {
	return &IPFSLockReqBuilder{LockRequestBuilder: b}
}
func (b *IPFSLockReqBuilder) ReadOneRep(nodeID int64, fileHash string) *IPFSLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(nodeID),
		Name:   lockprovider.IPFS_ELEMENT_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(fileHash),
	})
	return b
}

func (b *IPFSLockReqBuilder) WriteOneRep(nodeID int64, fileHash string) *IPFSLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(nodeID),
		Name:   lockprovider.IPFS_ELEMENT_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(fileHash),
	})
	return b
}

func (b *IPFSLockReqBuilder) ReadAnyRep(nodeID int64) *IPFSLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(nodeID),
		Name:   lockprovider.IPFS_SET_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}

func (b *IPFSLockReqBuilder) WriteAnyRep(nodeID int64) *IPFSLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(nodeID),
		Name:   lockprovider.IPFS_SET_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}

func (b *IPFSLockReqBuilder) CreateAnyRep(nodeID int64) *IPFSLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(nodeID),
		Name:   lockprovider.IPFS_SET_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}

func (b *IPFSLockReqBuilder) makePath(nodeID int64) []string {
	return []string{lockprovider.IPFSLockPathPrefix, strconv.FormatInt(nodeID, 10)}
}
