package reqbuilder

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/lockprovider"
)

type MetadataBucketLockReqBuilder struct {
	*MetadataLockReqBuilder
}

func (b *MetadataLockReqBuilder) Bucket() *MetadataBucketLockReqBuilder {
	return &MetadataBucketLockReqBuilder{MetadataLockReqBuilder: b}
}

func (b *MetadataBucketLockReqBuilder) ReadOne(bucketID cdssdk.BucketID) *MetadataBucketLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Bucket"),
		Name:   lockprovider.METADATA_ELEMENT_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(bucketID),
	})
	return b
}
func (b *MetadataBucketLockReqBuilder) WriteOne(bucketID cdssdk.BucketID) *MetadataBucketLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Bucket"),
		Name:   lockprovider.METADATA_ELEMENT_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(bucketID),
	})
	return b
}
func (b *MetadataBucketLockReqBuilder) CreateOne(userID cdssdk.UserID, bucketName string) *MetadataBucketLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Bucket"),
		Name:   lockprovider.METADATA_ELEMENT_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(userID, bucketName),
	})
	return b
}
func (b *MetadataBucketLockReqBuilder) ReadAny() *MetadataBucketLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Bucket"),
		Name:   lockprovider.METADATA_SET_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataBucketLockReqBuilder) WriteAny() *MetadataBucketLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Bucket"),
		Name:   lockprovider.METADATA_SET_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataBucketLockReqBuilder) CreateAny() *MetadataBucketLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Bucket"),
		Name:   lockprovider.METADATA_SET_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
