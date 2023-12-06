package reqbuilder

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/lockprovider"
)

type MetadataPackageLockReqBuilder struct {
	*MetadataLockReqBuilder
}

func (b *MetadataLockReqBuilder) Package() *MetadataPackageLockReqBuilder {
	return &MetadataPackageLockReqBuilder{MetadataLockReqBuilder: b}
}

func (b *MetadataPackageLockReqBuilder) ReadOne(packageID cdssdk.PackageID) *MetadataPackageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Package"),
		Name:   lockprovider.METADATA_ELEMENT_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(packageID),
	})
	return b
}
func (b *MetadataPackageLockReqBuilder) WriteOne(packageID cdssdk.PackageID) *MetadataPackageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Package"),
		Name:   lockprovider.METADATA_ELEMENT_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(packageID),
	})
	return b
}
func (b *MetadataPackageLockReqBuilder) CreateOne(bucketID cdssdk.BucketID, packageName string) *MetadataPackageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Package"),
		Name:   lockprovider.METADATA_ELEMENT_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(bucketID, packageName),
	})
	return b
}
func (b *MetadataPackageLockReqBuilder) ReadAny() *MetadataPackageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Package"),
		Name:   lockprovider.METADATA_SET_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataPackageLockReqBuilder) WriteAny() *MetadataPackageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Package"),
		Name:   lockprovider.METADATA_SET_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataPackageLockReqBuilder) CreateAny() *MetadataPackageLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Package"),
		Name:   lockprovider.METADATA_SET_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
