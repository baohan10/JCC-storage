package ops2

import (
	"context"
	"fmt"
	"io"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"golang.org/x/sync/semaphore"
)

func init() {
	exec.UseOp[*ChunkedSplit]()
	exec.UseOp[*ChunkedJoin]()
}

type ChunkedSplit struct {
	Input        *exec.StreamVar   `json:"input"`
	Outputs      []*exec.StreamVar `json:"outputs"`
	ChunkSize    int               `json:"chunkSize"`
	PaddingZeros bool              `json:"paddingZeros"`
}

func (o *ChunkedSplit) Execute(ctx context.Context, e *exec.Executor) error {
	err := e.BindVars(ctx, o.Input)
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
	exec.PutArrayVars(e, o.Outputs)

	return sem.Acquire(ctx, int64(len(outputs)))
}

type ChunkedJoin struct {
	Inputs    []*exec.StreamVar `json:"inputs"`
	Output    *exec.StreamVar   `json:"output"`
	ChunkSize int               `json:"chunkSize"`
}

func (o *ChunkedJoin) Execute(ctx context.Context, e *exec.Executor) error {
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
	o.Output.Stream = io2.AfterReadClosedOnce(io2.BufferedChunkedJoin(strReaders, o.ChunkSize), func(closer io.ReadCloser) {
		fut.SetVoid()
	})
	e.PutVars(o.Output)

	return fut.Wait(ctx)
}

type ChunkedSplitType struct {
	OutputCount int
	ChunkSize   int
}

func (t *ChunkedSplitType) InitNode(node *dag.Node) {
	dag.NodeDeclareInputStream(node, 1)
	for i := 0; i < t.OutputCount; i++ {
		dag.NodeNewOutputStream(node, &ioswitch2.VarProps{})
	}
}

func (t *ChunkedSplitType) GenerateOp(op *dag.Node) (exec.Op, error) {
	return &ChunkedSplit{
		Input: op.InputStreams[0].Var,
		Outputs: lo.Map(op.OutputStreams, func(v *dag.StreamVar, idx int) *exec.StreamVar {
			return v.Var
		}),
		ChunkSize:    t.ChunkSize,
		PaddingZeros: true,
	}, nil
}

func (t *ChunkedSplitType) String(node *dag.Node) string {
	return fmt.Sprintf("ChunkedSplit[%v]%v%v", t.ChunkSize, formatStreamIO(node), formatValueIO(node))
}

type ChunkedJoinType struct {
	InputCount int
	ChunkSize  int
}

func (t *ChunkedJoinType) InitNode(node *dag.Node) {
	dag.NodeDeclareInputStream(node, t.InputCount)
	dag.NodeNewOutputStream(node, &ioswitch2.VarProps{})
}

func (t *ChunkedJoinType) GenerateOp(op *dag.Node) (exec.Op, error) {
	return &ChunkedJoin{
		Inputs: lo.Map(op.InputStreams, func(v *dag.StreamVar, idx int) *exec.StreamVar {
			return v.Var
		}),
		Output:    op.OutputStreams[0].Var,
		ChunkSize: t.ChunkSize,
	}, nil
}

func (t *ChunkedJoinType) String(node *dag.Node) string {
	return fmt.Sprintf("ChunkedJoin[%v]%v%v", t.ChunkSize, formatStreamIO(node), formatValueIO(node))
}
