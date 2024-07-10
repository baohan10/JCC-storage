package ops

import (
	"context"
	"sync"

	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type Store struct {
	Var   ioswitch.Var
	Key   string
	Store *sync.Map
}

func (o *Store) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, o.Var)
	if err != nil {
		return err
	}

	switch v := o.Var.(type) {
	case *ioswitch.IntVar:
		o.Store.Store(o.Key, v.Value)
	case *ioswitch.StringVar:
		o.Store.Store(o.Key, v.Value)
	}

	return nil
}
