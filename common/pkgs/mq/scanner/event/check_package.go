package event

var _ = Register[*CheckPackage]()

type CheckPackage struct {
	EventBase
	PackageIDs []int64 `json:"packageIDs"`
}

func NewCheckPackage(packageIDs []int64) *CheckPackage {
	return &CheckPackage{
		PackageIDs: packageIDs,
	}
}
