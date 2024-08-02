package ops

import (
	"context"
	"io"

	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type DropStream struct {
	Input *ioswitch.StreamVar `json:"input"`
}

func (o *DropStream) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, o.Input)
	if err != nil {
		return err
	}

	for {
		buf := make([]byte, 1024*8)
		_, err = o.Input.Stream.Read(buf)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func init() {
	OpUnion.AddT((*DropStream)(nil))
}
