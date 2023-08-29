package reqbuilder

import (
	"strconv"

	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/lockprovider"
)

type StorageLockReqBuilder struct {
	*LockRequestBuilder
}

func (b *LockRequestBuilder) Storage() *StorageLockReqBuilder {
	return &StorageLockReqBuilder{LockRequestBuilder: b}
}

func (b *StorageLockReqBuilder) ReadOnePackage(storageID int64, userID int64, packageID int64) *StorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(storageID),
		Name:   lockprovider.STORAGE_ELEMENT_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(userID, packageID),
	})
	return b
}

func (b *StorageLockReqBuilder) WriteOnePackage(storageID int64, userID int64, packageID int64) *StorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(storageID),
		Name:   lockprovider.STORAGE_ELEMENT_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(userID, packageID),
	})
	return b
}

func (b *StorageLockReqBuilder) CreateOnePackage(storageID int64, userID int64, packageID int64) *StorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(storageID),
		Name:   lockprovider.STORAGE_ELEMENT_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(userID, packageID),
	})
	return b
}

func (b *StorageLockReqBuilder) ReadAnyPackage(storageID int64) *StorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(storageID),
		Name:   lockprovider.STORAGE_SET_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}

func (b *StorageLockReqBuilder) WriteAnyPackage(storageID int64) *StorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(storageID),
		Name:   lockprovider.STORAGE_SET_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}

func (b *StorageLockReqBuilder) CreateAnyPackage(storageID int64) *StorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(storageID),
		Name:   lockprovider.STORAGE_SET_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}

func (b *StorageLockReqBuilder) makePath(storageID int64) []string {
	return []string{lockprovider.StorageLockPathPrefix, strconv.FormatInt(storageID, 10)}
}
