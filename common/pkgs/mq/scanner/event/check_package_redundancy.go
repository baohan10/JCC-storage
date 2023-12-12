package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type CheckPackageRedundancy struct {
	EventBase
	PackageID cdssdk.PackageID `json:"packageIDs"`
}

func NewCheckPackageRedundancy(packageID cdssdk.PackageID) *CheckPackageRedundancy {
	return &CheckPackageRedundancy{
		PackageID: packageID,
	}
}

func init() {
	Register[*CheckPackageRedundancy]()
}
