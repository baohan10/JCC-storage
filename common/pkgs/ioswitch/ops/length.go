package ops

import (
	"context"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type Length struct {
	Input  *ioswitch.StreamVar `json:"input"`
	Output *ioswitch.StreamVar `json:"output"`
	Length int64               `json:"length"`
}

func (o *Length) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, o.Input)
	if err != nil {
		return err
	}
	defer o.Input.Stream.Close()

	fut := future.NewSetVoid()
	o.Output.Stream = io2.AfterReadClosedOnce(io2.Length(o.Input.Stream, o.Length), func(closer io.ReadCloser) {
		fut.SetVoid()
	})
	sw.PutVars(o.Output)

	return fut.Wait(ctx)
}

func init() {
	OpUnion.AddT((*Length)(nil))
}
