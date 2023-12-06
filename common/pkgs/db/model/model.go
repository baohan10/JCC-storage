package model

import (
	"time"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
)

// TODO 可以考虑逐步迁移到cdssdk中。迁移思路：数据对象应该包含的字段都迁移到cdssdk中，内部使用的一些特殊字段则留在这里

type Node struct {
	NodeID           cdssdk.NodeID     `db:"NodeID" json:"nodeID"`
	Name             string            `db:"Name" json:"name"`
	LocalIP          string            `db:"LocalIP" json:"localIP"`
	ExternalIP       string            `db:"ExternalIP" json:"externalIP"`
	LocalGRPCPort    int               `db:"LocalGRPCPort" json:"localGRPCPort"`
	ExternalGRPCPort int               `db:"ExternalGRPCPort" json:"externalGRPCPort"`
	LocationID       cdssdk.LocationID `db:"LocationID" json:"locationID"`
	State            string            `db:"State" json:"state"`
	LastReportTime   *time.Time        `db:"LastReportTime" json:"lastReportTime"`
}

type Storage struct {
	StorageID cdssdk.StorageID `db:"StorageID" json:"storageID"`
	Name      string           `db:"Name" json:"name"`
	NodeID    cdssdk.NodeID    `db:"NodeID" json:"nodeID"`
	Directory string           `db:"Directory" json:"directory"`
	State     string           `db:"State" json:"state"`
}

type NodeDelay struct {
	SourceNodeID      int64 `db:"SourceNodeID"`
	DestinationNodeID int64 `db:"DestinationNodeID"`
	DelayInMs         int   `db:"DelayInMs"`
}

type User struct {
	UserID   cdssdk.UserID `db:"UserID" json:"userID"`
	Password string        `db:"PassWord" json:"password"`
}

type UserBucket struct {
	UserID   cdssdk.UserID   `db:"UserID" json:"userID"`
	BucketID cdssdk.BucketID `db:"BucketID" json:"bucketID"`
}

type UserNode struct {
	UserID cdssdk.UserID `db:"UserID" json:"userID"`
	NodeID cdssdk.NodeID `db:"NodeID" json:"nodeID"`
}

type UserStorage struct {
	UserID    cdssdk.UserID    `db:"UserID" json:"userID"`
	StorageID cdssdk.StorageID `db:"StorageID" json:"storageID"`
}

type Bucket struct {
	BucketID  cdssdk.BucketID `db:"BucketID" json:"bucketID"`
	Name      string          `db:"Name" json:"name"`
	CreatorID cdssdk.UserID   `db:"CreatorID" json:"creatorID"`
}

type Package = cdssdk.Package

type Object = cdssdk.Object

type ObjectBlock = stgmod.ObjectBlock

type Cache struct {
	FileHash   string        `db:"FileHash" json:"fileHash"`
	NodeID     cdssdk.NodeID `db:"NodeID" json:"nodeID"`
	State      string        `db:"State" json:"state"`
	FrozenTime *time.Time    `db:"FrozenTime" json:"frozenTime"`
	CreateTime time.Time     `db:"CreateTime" json:"createTime"`
	Priority   int           `db:"Priority" json:"priority"`
}

const (
	StoragePackageStateNormal   = "Normal"
	StoragePackageStateDeleted  = "Deleted"
	StoragePackageStateOutdated = "Outdated"
)

// Storage当前加载的Package
type StoragePackage struct {
	StorageID cdssdk.StorageID `db:"StorageID" json:"storageID"`
	PackageID cdssdk.PackageID `db:"PackageID" json:"packageID"`
	UserID    cdssdk.UserID    `db:"UserID" json:"userID"`
	State     string           `db:"State" json:"state"`
}

type StoragePackageLog struct {
	StorageID  cdssdk.StorageID `db:"StorageID" json:"storageID"`
	PackageID  cdssdk.PackageID `db:"PackageID" json:"packageID"`
	UserID     cdssdk.UserID    `db:"UserID" json:"userID"`
	CreateTime time.Time        `db:"CreateTime" json:"createTime"`
}

type Location struct {
	LocationID cdssdk.LocationID `db:"LocationID" json:"locationID"`
	Name       string            `db:"Name" json:"name"`
}
