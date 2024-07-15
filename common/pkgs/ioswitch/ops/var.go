package ops

import (
	"context"

	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type ConstVar struct {
	Var *ioswitch.StringVar `json:"var"`
}

func (o *ConstVar) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	sw.PutVars(o.Var)
	return nil
}

func init() {
	OpUnion.AddT((*ConstVar)(nil))
}
