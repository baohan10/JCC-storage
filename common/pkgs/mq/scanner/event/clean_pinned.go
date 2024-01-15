package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type CleanPinned struct {
	EventBase
	PackageID cdssdk.PackageID `json:"nodeID"`
}

func NewCleanPinned(packageID cdssdk.PackageID) *CleanPinned {
	return &CleanPinned{
		PackageID: packageID,
	}
}

func init() {
	Register[*CleanPinned]()
}
