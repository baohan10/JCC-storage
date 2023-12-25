package reqbuilder

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/lockprovider"
)

type MetadataObjectLockReqBuilder struct {
	*MetadataLockReqBuilder
}

func (b *MetadataLockReqBuilder) Object() *MetadataObjectLockReqBuilder {
	return &MetadataObjectLockReqBuilder{MetadataLockReqBuilder: b}
}

func (b *MetadataObjectLockReqBuilder) CreateOne(packageID cdssdk.PackageID, objectPath string) *MetadataObjectLockReqBuilder {
	b.locks = append(b.locks, distlock.Lock{
		Path:   b.makePath("Object"),
		Name:   lockprovider.MetadataCreateLock,
		Target: *lockprovider.NewStringLockTarget().Add(packageID, objectPath),
	})
	return b
}
