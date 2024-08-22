package ops2

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
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec/lrc"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitchlrc"
)

func init() {
	exec.UseOp[*GalMultiply]()
}

type GalMultiply struct {
	Coef      [][]byte          `json:"coef"`
	Inputs    []*exec.StreamVar `json:"inputs"`
	Outputs   []*exec.StreamVar `json:"outputs"`
	ChunkSize int               `json:"chunkSize"`
}

func (o *GalMultiply) Execute(ctx context.Context, e *exec.Executor) error {
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

type LRCConstructAnyType struct {
	LRC cdssdk.LRCRedundancy
}

func (t *LRCConstructAnyType) InitNode(node *dag.Node) {}

func (t *LRCConstructAnyType) GenerateOp(op *dag.Node) (exec.Op, error) {
	var inputIdxs []int
	var outputIdxs []int
	for _, in := range op.InputStreams {
		inputIdxs = append(inputIdxs, ioswitch2.SProps(in).StreamIndex)
	}
	for _, out := range op.OutputStreams {
		outputIdxs = append(outputIdxs, ioswitch2.SProps(out).StreamIndex)
	}

	l, err := lrc.New(t.LRC.N, t.LRC.K, t.LRC.Groups)
	if err != nil {
		return nil, err
	}
	coef, err := l.GenerateMatrix(inputIdxs, outputIdxs)
	if err != nil {
		return nil, err
	}

	return &GalMultiply{
		Coef:      coef,
		Inputs:    lo.Map(op.InputStreams, func(v *dag.StreamVar, idx int) *exec.StreamVar { return v.Var }),
		Outputs:   lo.Map(op.OutputStreams, func(v *dag.StreamVar, idx int) *exec.StreamVar { return v.Var }),
		ChunkSize: t.LRC.ChunkSize,
	}, nil
}

func (t *LRCConstructAnyType) AddInput(node *dag.Node, str *dag.StreamVar) {
	node.InputStreams = append(node.InputStreams, str)
	str.To(node, len(node.InputStreams)-1)
}

func (t *LRCConstructAnyType) NewOutput(node *dag.Node, dataIndex int) *dag.StreamVar {
	return dag.NodeNewOutputStream(node, &ioswitch2.VarProps{StreamIndex: dataIndex})
}

func (t *LRCConstructAnyType) String(node *dag.Node) string {
	return fmt.Sprintf("LRCAny[]%v%v", formatStreamIO(node), formatValueIO(node))
}

type LRCConstructGroupType struct {
	LRC              cdssdk.LRCRedundancy
	TargetBlockIndex int
}

func (t *LRCConstructGroupType) InitNode(node *dag.Node) {
	dag.NodeNewOutputStream(node, &ioswitchlrc.VarProps{
		StreamIndex: t.TargetBlockIndex,
	})

	grpIdx := t.LRC.FindGroup(t.TargetBlockIndex)
	dag.NodeDeclareInputStream(node, t.LRC.Groups[grpIdx])
}

func (t *LRCConstructGroupType) GenerateOp(op *dag.Node) (exec.Op, error) {
	l, err := lrc.New(t.LRC.N, t.LRC.K, t.LRC.Groups)
	if err != nil {
		return nil, err
	}
	coef, err := l.GenerateGroupMatrix(t.TargetBlockIndex)
	if err != nil {
		return nil, err
	}

	return &GalMultiply{
		Coef:      coef,
		Inputs:    lo.Map(op.InputStreams, func(v *dag.StreamVar, idx int) *exec.StreamVar { return v.Var }),
		Outputs:   lo.Map(op.OutputStreams, func(v *dag.StreamVar, idx int) *exec.StreamVar { return v.Var }),
		ChunkSize: t.LRC.ChunkSize,
	}, nil
}

func (t *LRCConstructGroupType) String(node *dag.Node) string {
	return fmt.Sprintf("LRCGroup[]%v%v", formatStreamIO(node), formatValueIO(node))
}
