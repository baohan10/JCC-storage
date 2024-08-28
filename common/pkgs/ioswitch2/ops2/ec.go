package ops2

import (
	"context"
	"fmt"
	"io"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/utils"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/sync2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"golang.org/x/sync/semaphore"
)

func init() {
	// exec.UseOp[*ECReconstructAny]()
	// exec.UseOp[*ECReconstruct]()
	exec.UseOp[*ECMultiply]()
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

func (o *ECMultiply) String() string {
	return fmt.Sprintf(
		"ECMultiply(coef=%v) (%v) -> (%v)",
		o.Coef,
		utils.FormatVarIDs(o.Inputs),
		utils.FormatVarIDs(o.Outputs),
	)
}

type MultiplyType struct {
	EC            cdssdk.ECRedundancy
	InputIndexes  []int
	OutputIndexes []int
}

func (t *MultiplyType) InitNode(node *dag.Node) {}

func (t *MultiplyType) GenerateOp(op *dag.Node) (exec.Op, error) {
	rs, err := ec.NewRs(t.EC.K, t.EC.N)
	if err != nil {
		return nil, err
	}
	coef, err := rs.GenerateMatrix(t.InputIndexes, t.OutputIndexes)
	if err != nil {
		return nil, err
	}

	return &ECMultiply{
		Coef:      coef,
		Inputs:    lo.Map(op.InputStreams, func(v *dag.StreamVar, idx int) *exec.StreamVar { return v.Var }),
		Outputs:   lo.Map(op.OutputStreams, func(v *dag.StreamVar, idx int) *exec.StreamVar { return v.Var }),
		ChunkSize: t.EC.ChunkSize,
	}, nil
}

func (t *MultiplyType) AddInput(node *dag.Node, str *dag.StreamVar, dataIndex int) {
	t.InputIndexes = append(t.InputIndexes, dataIndex)
	node.InputStreams = append(node.InputStreams, str)
	str.To(node, len(node.InputStreams)-1)
}

func (t *MultiplyType) NewOutput(node *dag.Node, dataIndex int) *dag.StreamVar {
	t.OutputIndexes = append(t.OutputIndexes, dataIndex)
	return dag.NodeNewOutputStream(node, &ioswitch2.VarProps{StreamIndex: dataIndex})
}

func (t *MultiplyType) String(node *dag.Node) string {
	return fmt.Sprintf("Multiply[]%v%v", formatStreamIO(node), formatValueIO(node))
}
