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

func (b *MetadataStoragePackageLockReqBuilder) CreateOne(userID cdssdk.UserID, storageID cdssdk.StorageID, packageID cdssdk.PackageID) *MetadataStoragePackageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("StoragePackage"),
		Name:   lockprovider.MetadataCreateLock,
		Target: *lockprovider.NewStringLockTarget().Add(userID, storageID, packageID),
	})
	return b
}
