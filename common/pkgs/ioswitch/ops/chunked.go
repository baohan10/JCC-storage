package ops

import (
	"context"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	"golang.org/x/sync/semaphore"
)

type ChunkedSplit struct {
	Input        *ioswitch.StreamVar   `json:"input"`
	Outputs      []*ioswitch.StreamVar `json:"outputs"`
	ChunkSize    int                   `json:"chunkSize"`
	PaddingZeros bool                  `json:"paddingZeros"`
}

func (o *ChunkedSplit) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, o.Input)
	if err != nil {
		return err
	}
	defer o.Input.Stream.Close()

	outputs := io2.ChunkedSplit(o.Input.Stream, o.ChunkSize, len(o.Outputs), io2.ChunkedSplitOption{
		PaddingZeros: o.PaddingZeros,
	})

	sem := semaphore.NewWeighted(int64(len(outputs)))
	for i := range outputs {
		sem.Acquire(ctx, 1)

		o.Outputs[i].Stream = io2.AfterReadClosedOnce(outputs[i], func(closer io.ReadCloser) {
			sem.Release(1)
		})
	}
	ioswitch.PutArrayVars(sw, o.Outputs)

	return sem.Acquire(ctx, int64(len(outputs)))
}

type ChunkedJoin struct {
	Inputs    []*ioswitch.StreamVar `json:"inputs"`
	Output    *ioswitch.StreamVar   `json:"output"`
	ChunkSize int                   `json:"chunkSize"`
}

func (o *ChunkedJoin) Execute(ctx context.Context, sw *ioswitch.Switch) error {
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
	o.Output.Stream = io2.AfterReadClosedOnce(io2.ChunkedJoin(strReaders, o.ChunkSize), func(closer io.ReadCloser) {
		fut.SetVoid()
	})
	sw.PutVars(o.Output)

	return fut.Wait(ctx)
}

func init() {
	OpUnion.AddT((*ChunkedSplit)(nil))
	OpUnion.AddT((*ChunkedJoin)(nil))
}
