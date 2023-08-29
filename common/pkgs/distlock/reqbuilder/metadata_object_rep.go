package reqbuilder

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/lockprovider"
)

type MetadataObjectRepLockReqBuilder struct {
	*MetadataLockReqBuilder
}

func (b *MetadataLockReqBuilder) ObjectRep() *MetadataObjectRepLockReqBuilder {
	return &MetadataObjectRepLockReqBuilder{MetadataLockReqBuilder: b}
}

func (b *MetadataObjectRepLockReqBuilder) ReadOne(objectID int64) *MetadataObjectRepLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("ObjectRep"),
		Name:   lockprovider.METADATA_ELEMENT_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(objectID),
	})
	return b
}
func (b *MetadataObjectRepLockReqBuilder) WriteOne(objectID int64) *MetadataObjectRepLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("ObjectRep"),
		Name:   lockprovider.METADATA_ELEMENT_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(objectID),
	})
	return b
}
func (b *MetadataObjectRepLockReqBuilder) CreateOne() *MetadataObjectRepLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("ObjectRep"),
		Name:   lockprovider.METADATA_ELEMENT_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataObjectRepLockReqBuilder) ReadAny() *MetadataObjectRepLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("ObjectRep"),
		Name:   lockprovider.METADATA_SET_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataObjectRepLockReqBuilder) WriteAny() *MetadataObjectRepLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("ObjectRep"),
		Name:   lockprovider.METADATA_SET_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataObjectRepLockReqBuilder) CreateAny() *MetadataObjectRepLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("ObjectRep"),
		Name:   lockprovider.METADATA_SET_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
