package model

import (
	"time"

	stgsdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

// TODO 可以考虑逐步迁移到stgsdk中。迁移思路：数据对象应该包含的字段都迁移到stgsdk中，内部使用的一些特殊字段则留在这里

type Node struct {
	NodeID           int64      `db:"NodeID" json:"nodeID"`
	Name             string     `db:"Name" json:"name"`
	LocalIP          string     `db:"LocalIP" json:"localIP"`
	ExternalIP       string     `db:"ExternalIP" json:"externalIP"`
	LocalGRPCPort    int        `db:"LocalGRPCPort" json:"localGRPCPort"`
	ExternalGRPCPort int        `db:"ExternalGRPCPort" json:"externalGRPCPort"`
	LocationID       int64      `db:"LocationID" json:"locationID"`
	State            string     `db:"State" json:"state"`
	LastReportTime   *time.Time `db:"LastReportTime" json:"lastReportTime"`
}

type Storage struct {
	StorageID int64  `db:"StorageID" json:"storageID"`
	Name      string `db:"Name" json:"name"`
	NodeID    int64  `db:"NodeID" json:"nodeID"`
	Directory string `db:"Directory" json:"directory"`
	State     string `db:"State" json:"state"`
}

type NodeDelay struct {
	SourceNodeID      int64 `db:"SourceNodeID"`
	DestinationNodeID int64 `db:"DestinationNodeID"`
	DelayInMs         int   `db:"DelayInMs"`
}

type User struct {
	UserID   int64  `db:"UserID" json:"userID"`
	Password string `db:"PassWord" json:"password"`
}

type UserBucket struct {
	UserID   int64 `db:"UserID" json:"userID"`
	BucketID int64 `db:"BucketID" json:"bucketID"`
}

type UserNode struct {
	UserID int64 `db:"UserID" json:"userID"`
	NodeID int64 `db:"NodeID" json:"nodeID"`
}

type UserStorage struct {
	UserID    int64 `db:"UserID" json:"userID"`
	StorageID int64 `db:"StorageID" json:"storageID"`
}

type Bucket struct {
	BucketID  int64  `db:"BucketID" json:"bucketID"`
	Name      string `db:"Name" json:"name"`
	CreatorID int64  `db:"CreatorID" json:"creatorID"`
}

type Package = stgsdk.Package

type Object = stgsdk.Object

type ObjectRep struct {
	ObjectID int64  `db:"ObjectID" json:"objectID"`
	FileHash string `db:"FileHash" json:"fileHash"`
}

type ObjectBlock struct {
	ObjectID int64  `db:"ObjectID" json:"objectID"`
	Index    int    `db:"Index" json:"index"`
	FileHash string `db:"FileHash" json:"fileHash"`
}

type Cache struct {
	FileHash  string    `db:"FileHash" json:"fileHash"`
	NodeID    int64     `db:"NodeID" json:"nodeID"`
	State     string    `db:"State" json:"state"`
	CacheTime time.Time `db:"CacheTime" json:"cacheTime"`
	Priority  int       `db:"Priority" json:"priority"`
}

type StoragePackage struct {
	PackageID int64  `db:"PackageID" json:"packageID"`
	StorageID int64  `db:"StorageID" json:"storageID"`
	UserID    int64  `db:"UserID" json:"userID"`
	State     string `db:"State" json:"state"`
}

type Location struct {
	LocationID int64  `db:"LocationID" json:"locationID"`
	Name       string `db:"Name" json:"name"`
}

type Ec struct {
	EcID int    `db:"EcID" json:"ecID"`
	Name string `db:"Name" json:"name"`
	EcK  int    `db:"EcK" json:"ecK"`
	EcN  int    `db:"EcN" json:"ecN"`
}
