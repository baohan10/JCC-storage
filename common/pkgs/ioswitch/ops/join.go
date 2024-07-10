package ops

import (
	"context"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type Join struct {
	Inputs []*ioswitch.StreamVar `json:"inputs"`
	Output *ioswitch.StreamVar   `json:"output"`
	Length int64                 `json:"length"`
}

func (o *Join) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := ioswitch.BindArrayVars(sw, ctx, o.Inputs)
	if err != nil {
		return err
	}

	var strReaders []io.Reader
	for _, s := range o.Inputs {
		strReaders = append(strReaders, s.Stream)
	}
	defer func() {
		for _, str := range o.Inputs {
			str.Stream.Close()
		}
	}()

	fut := future.NewSetVoid()
	o.Output.Stream = io2.AfterReadClosedOnce(io2.Length(io2.Join(strReaders), o.Length), func(closer io.ReadCloser) {
		fut.SetVoid()
	})
	sw.PutVars(o.Output)

	return fut.Wait(ctx)
}

func init() {
	OpUnion.AddT((*Join)(nil))
}
