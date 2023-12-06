package reqbuilder

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/lockprovider"
)

type MetadataUserStorageLockReqBuilder struct {
	*MetadataLockReqBuilder
}

func (b *MetadataLockReqBuilder) UserStorage() *MetadataUserStorageLockReqBuilder {
	return &MetadataUserStorageLockReqBuilder{MetadataLockReqBuilder: b}
}

func (b *MetadataUserStorageLockReqBuilder) ReadOne(userID cdssdk.UserID, storageID cdssdk.StorageID) *MetadataUserStorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("UserStorage"),
		Name:   lockprovider.METADATA_ELEMENT_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(userID, storageID),
	})
	return b
}
func (b *MetadataUserStorageLockReqBuilder) WriteOne(userID cdssdk.UserID, storageID cdssdk.StorageID) *MetadataUserStorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("UserStorage"),
		Name:   lockprovider.METADATA_ELEMENT_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(userID, storageID),
	})
	return b
}
func (b *MetadataUserStorageLockReqBuilder) CreateOne(userID cdssdk.UserID, storageID cdssdk.StorageID) *MetadataUserStorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("UserStorage"),
		Name:   lockprovider.METADATA_ELEMENT_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(userID, storageID),
	})
	return b
}
func (b *MetadataUserStorageLockReqBuilder) ReadAny() *MetadataUserStorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("UserStorage"),
		Name:   lockprovider.METADATA_SET_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataUserStorageLockReqBuilder) WriteAny() *MetadataUserStorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("UserStorage"),
		Name:   lockprovider.METADATA_SET_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataUserStorageLockReqBuilder) CreateAny() *MetadataUserStorageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("UserStorage"),
		Name:   lockprovider.METADATA_SET_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
