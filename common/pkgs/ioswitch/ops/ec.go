package ops

import (
	"context"
	"fmt"
	"io"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/sync2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
	"golang.org/x/sync/semaphore"
)

func init() {
	OpUnion.AddT((*ECReconstructAny)(nil))
	OpUnion.AddT((*ECReconstruct)(nil))
	OpUnion.AddT((*ECMultiply)(nil))
}

type ECReconstructAny struct {
	EC                 cdssdk.ECRedundancy `json:"ec"`
	Inputs             []*exec.StreamVar   `json:"inputs"`
	Outputs            []*exec.StreamVar   `json:"outputs"`
	InputBlockIndexes  []int               `json:"inputBlockIndexes"`
	OutputBlockIndexes []int               `json:"outputBlockIndexes"`
}

func (o *ECReconstructAny) Execute(ctx context.Context, e *exec.Executor) error {
	rs, err := ec.NewStreamRs(o.EC.K, o.EC.N, o.EC.ChunkSize)
	if err != nil {
		return fmt.Errorf("new ec: %w", err)
	}

	err = exec.BindArrayVars(e, ctx, o.Inputs)
	if err != nil {
		return err
	}
	defer func() {
		for _, s := range o.Inputs {
			s.Stream.Close()
		}
	}()

	var inputs []io.Reader
	for _, s := range o.Inputs {
		inputs = append(inputs, s.Stream)
	}

	outputs := rs.ReconstructAny(inputs, o.InputBlockIndexes, o.OutputBlockIndexes)

	sem := semaphore.NewWeighted(int64(len(o.Outputs)))
	for i := range o.Outputs {
		sem.Acquire(ctx, 1)

		o.Outputs[i].Stream = io2.AfterReadClosedOnce(outputs[i], func(closer io.ReadCloser) {
			sem.Release(1)
		})
	}
	exec.PutArrayVars(e, o.Outputs)

	return sem.Acquire(ctx, int64(len(o.Outputs)))
}

type ECReconstruct struct {
	EC                cdssdk.ECRedundancy `json:"ec"`
	Inputs            []*exec.StreamVar   `json:"inputs"`
	Outputs           []*exec.StreamVar   `json:"outputs"`
	InputBlockIndexes []int               `json:"inputBlockIndexes"`
}

func (o *ECReconstruct) Execute(ctx context.Context, e *exec.Executor) error {
	rs, err := ec.NewStreamRs(o.EC.K, o.EC.N, o.EC.ChunkSize)
	if err != nil {
		return fmt.Errorf("new ec: %w", err)
	}

	err = exec.BindArrayVars(e, ctx, o.Inputs)
	if err != nil {
		return err
	}
	defer func() {
		for _, s := range o.Inputs {
			s.Stream.Close()
		}
	}()

	var inputs []io.Reader
	for _, s := range o.Inputs {
		inputs = append(inputs, s.Stream)
	}

	outputs := rs.ReconstructData(inputs, o.InputBlockIndexes)

	sem := semaphore.NewWeighted(int64(len(o.Outputs)))
	for i := range o.Outputs {
		sem.Acquire(ctx, 1)

		o.Outputs[i].Stream = io2.AfterReadClosedOnce(outputs[i], func(closer io.ReadCloser) {
			sem.Release(1)
		})
	}
	exec.PutArrayVars(e, o.Outputs)

	return sem.Acquire(ctx, int64(len(o.Outputs)))
}

type ECMultiply struct {
	Coef      [][]byte          `json:"coef"`
	Inputs    []*exec.StreamVar `json:"inputs"`
	Outputs   []*exec.StreamVar `json:"outputs"`
	ChunkSize int               `json:"chunkSize"`
}

func (o *ECMultiply) Execute(ctx context.Context, e *exec.Executor) error {
	err := exec.BindArrayVars(e, ctx, o.Inputs)
	if err != nil {
		return err
	}
	defer func() {
		for _, s := range o.Inputs {
			s.Stream.Close()
		}
	}()

	outputWrs := make([]*io.PipeWriter, len(o.Outputs))

	for i := range o.Outputs {
		rd, wr := io.Pipe()
		o.Outputs[i].Stream = rd
		outputWrs[i] = wr
	}

	fut := future.NewSetVoid()
	go func() {
		mul := ec.GaloisMultiplier().BuildGalois()

		inputChunks := make([][]byte, len(o.Inputs))
		for i := range o.Inputs {
			inputChunks[i] = make([]byte, o.ChunkSize)
		}
		outputChunks := make([][]byte, len(o.Outputs))
		for i := range o.Outputs {
			outputChunks[i] = make([]byte, o.ChunkSize)
		}

		for {
			err := sync2.ParallelDo(o.Inputs, func(s *exec.StreamVar, i int) error {
				_, err := io.ReadFull(s.Stream, inputChunks[i])
				return err
			})
			if err == io.EOF {
				fut.SetVoid()
				return
			}
			if err != nil {
				fut.SetError(err)
				return
			}

			err = mul.Multiply(o.Coef, inputChunks, outputChunks)
			if err != nil {
				fut.SetError(err)
				return
			}

			for i := range o.Outputs {
				err := io2.WriteAll(outputWrs[i], outputChunks[i])
				if err != nil {
					fut.SetError(err)
					return
				}
			}
		}
	}()

	exec.PutArrayVars(e, o.Outputs)
	err = fut.Wait(ctx)
	if err != nil {
		for _, wr := range outputWrs {
			wr.CloseWithError(err)
		}
		return err
	}

	for _, wr := range outputWrs {
		wr.Close()
	}
	return nil
}

type MultiplyType struct {
	EC cdssdk.ECRedundancy
}

func (t *MultiplyType) InitNode(node *Node) {}

func (t *MultiplyType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	var inputIdxs []int
	var outputIdxs []int
	for _, in := range op.InputStreams {
		inputIdxs = append(inputIdxs, in.Props.StreamIndex)
	}
	for _, out := range op.OutputStreams {
		outputIdxs = append(outputIdxs, out.Props.StreamIndex)
	}

	rs, err := ec.NewRs(t.EC.K, t.EC.N)
	coef, err := rs.GenerateMatrix(inputIdxs, outputIdxs)
	if err != nil {
		return err
	}

	addOpByEnv(&ECMultiply{
		Coef:      coef,
		Inputs:    lo.Map(op.InputStreams, func(v *StreamVar, idx int) *exec.StreamVar { return v.Props.Var.(*exec.StreamVar) }),
		Outputs:   lo.Map(op.OutputStreams, func(v *StreamVar, idx int) *exec.StreamVar { return v.Props.Var.(*exec.StreamVar) }),
		ChunkSize: t.EC.ChunkSize,
	}, op.Env, blder)
	return nil
}

func (t *MultiplyType) AddInput(node *Node, str *StreamVar) {
	node.InputStreams = append(node.InputStreams, str)
	str.To(node, len(node.InputStreams)-1)
}

func (t *MultiplyType) NewOutput(node *Node, dataIndex int) *StreamVar {
	return dag.NodeNewOutputStream(node, VarProps{StreamIndex: dataIndex})
}

func (t *MultiplyType) String(node *Node) string {
	return fmt.Sprintf("Multiply[]%v%v", formatStreamIO(node), formatValueIO(node))
}
