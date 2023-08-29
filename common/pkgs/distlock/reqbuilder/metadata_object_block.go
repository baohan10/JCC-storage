package reqbuilder

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage-common/pkgs/distlock/lockprovider"
)

type MetadataObjectBlockLockReqBuilder struct {
	*MetadataLockReqBuilder
}

func (b *MetadataLockReqBuilder) ObjectBlock() *MetadataObjectBlockLockReqBuilder {
	return &MetadataObjectBlockLockReqBuilder{MetadataLockReqBuilder: b}
}

func (b *MetadataObjectBlockLockReqBuilder) ReadOne(objectID int) *MetadataObjectBlockLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("ObjectBlock"),
		Name:   lockprovider.METADATA_ELEMENT_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(objectID),
	})
	return b
}
func (b *MetadataObjectBlockLockReqBuilder) WriteOne(objectID int) *MetadataObjectBlockLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("ObjectBlock"),
		Name:   lockprovider.METADATA_ELEMENT_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(objectID),
	})
	return b
}
func (b *MetadataObjectBlockLockReqBuilder) CreateOne() *MetadataObjectBlockLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("ObjectBlock"),
		Name:   lockprovider.METADATA_ELEMENT_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataObjectBlockLockReqBuilder) ReadAny() *MetadataObjectBlockLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("ObjectBlock"),
		Name:   lockprovider.METADATA_SET_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataObjectBlockLockReqBuilder) WriteAny() *MetadataObjectBlockLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("ObjectBlock"),
		Name:   lockprovider.METADATA_SET_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataObjectBlockLockReqBuilder) CreateAny() *MetadataObjectBlockLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("ObjectBlock"),
		Name:   lockprovider.METADATA_SET_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
