package reqbuilder

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/lockprovider"
)

type MetadataStoragePackageLockReqBuilder struct {
	*MetadataLockReqBuilder
}

func (b *MetadataLockReqBuilder) StoragePackage() *MetadataStoragePackageLockReqBuilder {
	return &MetadataStoragePackageLockReqBuilder{MetadataLockReqBuilder: b}
}

func (b *MetadataStoragePackageLockReqBuilder) ReadOne(storageID cdssdk.StorageID, userID cdssdk.UserID, packageID cdssdk.PackageID) *MetadataStoragePackageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("StoragePackage"),
		Name:   lockprovider.METADATA_ELEMENT_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(storageID, userID, packageID),
	})
	return b
}
func (b *MetadataStoragePackageLockReqBuilder) WriteOne(storageID cdssdk.StorageID, userID cdssdk.UserID, packageID cdssdk.PackageID) *MetadataStoragePackageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("StoragePackage"),
		Name:   lockprovider.METADATA_ELEMENT_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(storageID, userID, packageID),
	})
	return b
}
func (b *MetadataStoragePackageLockReqBuilder) CreateOne(storageID cdssdk.StorageID, userID cdssdk.UserID, packageID cdssdk.PackageID) *MetadataStoragePackageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("StoragePackage"),
		Name:   lockprovider.METADATA_ELEMENT_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(storageID, userID, packageID),
	})
	return b
}
func (b *MetadataStoragePackageLockReqBuilder) ReadAny() *MetadataStoragePackageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("StoragePackage"),
		Name:   lockprovider.METADATA_SET_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataStoragePackageLockReqBuilder) WriteAny() *MetadataStoragePackageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("StoragePackage"),
		Name:   lockprovider.METADATA_SET_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataStoragePackageLockReqBuilder) CreateAny() *MetadataStoragePackageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("StoragePackage"),
		Name:   lockprovider.METADATA_SET_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
