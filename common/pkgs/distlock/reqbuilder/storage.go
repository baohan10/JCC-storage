package reqbuilder

import (
	"strconv"

	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/lockprovider"
)

type StorageLockReqBuilder struct {
	*LockRequestBuilder
}

func (b *LockRequestBuilder) Storage() *StorageLockReqBuilder {
	return &StorageLockReqBuilder{LockRequestBuilder: b}
}

func (b *StorageLockReqBuilder) Buzy(storageID cdssdk.StorageID) *StorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(storageID),
		Name:   lockprovider.StorageBuzyLock,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}

func (b *StorageLockReqBuilder) GC(storageID cdssdk.StorageID) *StorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(storageID),
		Name:   lockprovider.StorageGCLock,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}

func (b *StorageLockReqBuilder) makePath(storageID cdssdk.StorageID) []string {
	return []string{lockprovider.StorageLockPathPrefix, strconv.FormatInt(int64(storageID), 10)}
}
