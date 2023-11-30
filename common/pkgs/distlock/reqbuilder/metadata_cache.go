package reqbuilder

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/lockprovider"
)

type MetadataCacheLockReqBuilder struct {
	*MetadataLockReqBuilder
}

func (b *MetadataLockReqBuilder) Cache() *MetadataCacheLockReqBuilder {
	return &MetadataCacheLockReqBuilder{MetadataLockReqBuilder: b}
}

func (b *MetadataCacheLockReqBuilder) ReadOne(nodeID cdssdk.NodeID, fileHash string) *MetadataCacheLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Cache"),
		Name:   lockprovider.METADATA_ELEMENT_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(nodeID, fileHash),
	})
	return b
}
func (b *MetadataCacheLockReqBuilder) WriteOne(nodeID cdssdk.NodeID, fileHash string) *MetadataCacheLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Cache"),
		Name:   lockprovider.METADATA_ELEMENT_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(nodeID, fileHash),
	})
	return b
}
func (b *MetadataCacheLockReqBuilder) CreateOne(nodeID cdssdk.NodeID, fileHash string) *MetadataCacheLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Cache"),
		Name:   lockprovider.METADATA_ELEMENT_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget().Add(nodeID, fileHash),
	})
	return b
}
func (b *MetadataCacheLockReqBuilder) ReadAny() *MetadataCacheLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Cache"),
		Name:   lockprovider.METADATA_SET_READ_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataCacheLockReqBuilder) WriteAny() *MetadataCacheLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Cache"),
		Name:   lockprovider.METADATA_SET_WRITE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
func (b *MetadataCacheLockReqBuilder) CreateAny() *MetadataCacheLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Cache"),
		Name:   lockprovider.METADATA_SET_CREATE_LOCK,
		Target: *lockprovider.NewStringLockTarget(),
	})
	return b
}
