package reqbuilder

import (
	"gitlink.org.cn/cloudream/storage-common/pkgs/distlock/lockprovider"
)

type MetadataLockReqBuilder struct {
	*LockRequestBuilder
}

func (b *LockRequestBuilder) Metadata() *MetadataLockReqBuilder {
	return &MetadataLockReqBuilder{LockRequestBuilder: b}
}

func (b *MetadataLockReqBuilder) makePath(tableName string) []string {
	return []string{lockprovider.MetadataLockPathPrefix, tableName}
}
