package event

import cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

type CheckPackage struct {
	EventBase
	PackageIDs []cdssdk.PackageID `json:"packageIDs"`
}

func NewCheckPackage(packageIDs []cdssdk.PackageID) *CheckPackage {
	return &CheckPackage{
		PackageIDs: packageIDs,
	}
}

func init() {
	Register[*CheckPackage]()
}
