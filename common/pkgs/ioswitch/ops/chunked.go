package ops

import (
	"context"
	"fmt"
	"io"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"golang.org/x/sync/semaphore"
)

func init() {
	OpUnion.AddT((*ChunkedSplit)(nil))
	OpUnion.AddT((*ChunkedJoin)(nil))
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

func (t *ChunkedSplitType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, 1)
	for i := 0; i < t.OutputCount; i++ {
		dag.NodeNewOutputStream(node, VarProps{})
	}
}

func (t *ChunkedSplitType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&ChunkedSplit{
		Input: op.InputStreams[0].Props.Var.(*exec.StreamVar),
		Outputs: lo.Map(op.OutputStreams, func(v *StreamVar, idx int) *exec.StreamVar {
			return v.Props.Var.(*exec.StreamVar)
		}),
		ChunkSize:    t.ChunkSize,
		PaddingZeros: true,
	}, op.Env, blder)
	return nil
}

func (t *ChunkedSplitType) String(node *Node) string {
	return fmt.Sprintf("ChunkedSplit[%v]", t.ChunkSize, formatStreamIO(node), formatValueIO(node))
}

type ChunkedJoinType struct {
	InputCount int
	ChunkSize  int
}

func (t *ChunkedJoinType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, t.InputCount)
	dag.NodeNewOutputStream(node, VarProps{})
}

func (t *ChunkedJoinType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&ChunkedJoin{
		Inputs: lo.Map(op.InputStreams, func(v *StreamVar, idx int) *exec.StreamVar {
			return v.Props.Var.(*exec.StreamVar)
		}),
		Output:    op.OutputStreams[0].Props.Var.(*exec.StreamVar),
		ChunkSize: t.ChunkSize,
	}, op.Env, blder)
	return nil
}

func (t *ChunkedJoinType) String(node *Node) string {
	return fmt.Sprintf("ChunkedJoin[%v]", t.ChunkSize, formatStreamIO(node), formatValueIO(node))
}
