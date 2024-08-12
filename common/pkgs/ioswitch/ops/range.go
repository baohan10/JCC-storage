package ops

import (
	"context"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type Range struct {
	Input  *ioswitch.StreamVar `json:"input"`
	Output *ioswitch.StreamVar `json:"output"`
	Offset int64               `json:"offset"`
	Length *int64              `json:"length"`
}

func (o *Range) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, o.Input)
	if err != nil {
		return err
	}
	defer o.Input.Stream.Close()

	buf := make([]byte, 1024*16)

	// 跳过前Offset个字节
	for o.Offset > 0 {
		rdCnt := math2.Min(o.Offset, int64(len(buf)))
		rd, err := o.Input.Stream.Read(buf[:rdCnt])
		if err == io.EOF {
			// 输入流不够长度也不报错，只是产生一个空的流
			break
		}
		if err != nil {
			return err
		}
		o.Offset -= int64(rd)
	}

	fut := future.NewSetVoid()

	if o.Length == nil {
		o.Output.Stream = io2.AfterEOF(o.Input.Stream, func(closer io.ReadCloser, err error) {
			fut.SetVoid()
		})

		sw.PutVars(o.Output)
		return fut.Wait(ctx)
	}

	o.Output.Stream = io2.AfterEOF(io2.Length(o.Input.Stream, *o.Length), func(closer io.ReadCloser, err error) {
		fut.SetVoid()
	})

	sw.PutVars(o.Output)
	err = fut.Wait(ctx)
	if err != nil {
		return err
	}

	io2.DropWithBuf(o.Input.Stream, buf)
	return nil
}

func init() {
	OpUnion.AddT((*Range)(nil))
}
