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

func (b *StorageLockReqBuilder) ReadOnePackage(storageID cdssdk.StorageID, userID cdssdk.UserID, packageID cdssdk.PackageID) *StorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(storageID),
		Name:   lockprovider.STORAGE_ELEMENT_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(userID, packageID),
	})
	return b
}

func (b *StorageLockReqBuilder) WriteOnePackage(storageID cdssdk.StorageID, userID cdssdk.UserID, packageID cdssdk.PackageID) *StorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(storageID),
		Name:   lockprovider.STORAGE_ELEMENT_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(userID, packageID),
	})
	return b
}

func (b *StorageLockReqBuilder) CreateOnePackage(storageID cdssdk.StorageID, userID cdssdk.UserID, packageID cdssdk.PackageID) *StorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(storageID),
		Name:   lockprovider.STORAGE_ELEMENT_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(userID, packageID),
	})
	return b
}

func (b *StorageLockReqBuilder) ReadAnyPackage(storageID cdssdk.StorageID) *StorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(storageID),
		Name:   lockprovider.STORAGE_SET_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}

func (b *StorageLockReqBuilder) WriteAnyPackage(storageID cdssdk.StorageID) *StorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(storageID),
		Name:   lockprovider.STORAGE_SET_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}

func (b *StorageLockReqBuilder) CreateAnyPackage(storageID cdssdk.StorageID) *StorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath(storageID),
		Name:   lockprovider.STORAGE_SET_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}

func (b *StorageLockReqBuilder) makePath(storageID cdssdk.StorageID) []string {
	return []string{lockprovider.StorageLockPathPrefix, strconv.FormatInt(int64(storageID), 10)}
}
