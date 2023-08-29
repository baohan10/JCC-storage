package event

type CheckPackage struct {
	PackageIDs []int64 `json:"packageIDs"`
}

func NewCheckPackage(packageIDs []int64) CheckPackage {
	return CheckPackage{
		PackageIDs: packageIDs,
	}
}

func init() {
	Register[CheckPackage]()
}
