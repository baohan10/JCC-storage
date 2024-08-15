package ops

import (
	"context"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/utils/io2"
)

type Join struct {
	Inputs []*exec.StreamVar `json:"inputs"`
	Output *exec.StreamVar   `json:"output"`
	Length int64             `json:"length"`
}

func (o *Join) Execute(ctx context.Context, e *exec.Executor) error {
	err := exec.BindArrayVars(e, ctx, o.Inputs)
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
	e.PutVars(o.Output)

	return fut.Wait(ctx)
}

func init() {
	OpUnion.AddT((*Join)(nil))
}
