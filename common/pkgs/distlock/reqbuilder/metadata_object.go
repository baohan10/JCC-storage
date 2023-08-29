package reqbuilder

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/lockprovider"
)

// TODO 可以考虑增加基于PackageID的锁，让访问不同Package的Object的操作能并行

type MetadataObjectLockReqBuilder struct {
	*MetadataLockReqBuilder
}

func (b *MetadataLockReqBuilder) Object() *MetadataObjectLockReqBuilder {
	return &MetadataObjectLockReqBuilder{MetadataLockReqBuilder: b}
}

func (b *MetadataObjectLockReqBuilder) ReadOne(objectID int64) *MetadataObjectLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Object"),
		Name:   lockprovider.METADATA_ELEMENT_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(objectID),
	})
	return b
}
func (b *MetadataObjectLockReqBuilder) WriteOne(objectID int64) *MetadataObjectLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Object"),
		Name:   lockprovider.METADATA_ELEMENT_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(objectID),
	})
	return b
}
func (b *MetadataObjectLockReqBuilder) CreateOne(bucketID int64, objectName string) *MetadataObjectLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Object"),
		Name:   lockprovider.METADATA_ELEMENT_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(bucketID, objectName),
	})
	return b
}
func (b *MetadataObjectLockReqBuilder) ReadAny() *MetadataObjectLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Object"),
		Name:   lockprovider.METADATA_SET_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataObjectLockReqBuilder) WriteAny() *MetadataObjectLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Object"),
		Name:   lockprovider.METADATA_SET_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataObjectLockReqBuilder) CreateAny() *MetadataObjectLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Object"),
		Name:   lockprovider.METADATA_SET_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
