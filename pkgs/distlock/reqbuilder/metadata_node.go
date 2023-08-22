package reqbuilder

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/storage-common/pkgs/distlock/lockprovider"
)

type MetadataNodeLockReqBuilder struct {
	*MetadataLockReqBuilder
}

func (b *MetadataLockReqBuilder) Node() *MetadataNodeLockReqBuilder {
	return &MetadataNodeLockReqBuilder{MetadataLockReqBuilder: b}
}

func (b *MetadataNodeLockReqBuilder) ReadOne(nodeID int64) *MetadataNodeLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Node"),
		Name:   lockprovider.METADATA_ELEMENT_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(nodeID),
	})
	return b
}
func (b *MetadataNodeLockReqBuilder) WriteOne(nodeID int64) *MetadataNodeLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Node"),
		Name:   lockprovider.METADATA_ELEMENT_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(nodeID),
	})
	return b
}
func (b *MetadataNodeLockReqBuilder) CreateOne() *MetadataNodeLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Node"),
		Name:   lockprovider.METADATA_ELEMENT_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataNodeLockReqBuilder) ReadAny() *MetadataNodeLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Node"),
		Name:   lockprovider.METADATA_SET_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataNodeLockReqBuilder) WriteAny() *MetadataNodeLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Node"),
		Name:   lockprovider.METADATA_SET_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataNodeLockReqBuilder) CreateAny() *MetadataNodeLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Node"),
		Name:   lockprovider.METADATA_SET_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
