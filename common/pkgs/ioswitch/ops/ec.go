package ops

import (
	"context"
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/sync2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	"golang.org/x/sync/semaphore"
)

type ECReconstructAny struct {
	EC                 cdssdk.ECRedundancy   `json:"ec"`
	Inputs             []*ioswitch.StreamVar `json:"inputs"`
	Outputs            []*ioswitch.StreamVar `json:"outputs"`
	InputBlockIndexes  []int                 `json:"inputBlockIndexes"`
	OutputBlockIndexes []int                 `json:"outputBlockIndexes"`
}

func (o *ECReconstructAny) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	rs, err := ec.NewStreamRs(o.EC.K, o.EC.N, o.EC.ChunkSize)
	if err != nil {
		return fmt.Errorf("new ec: %w", err)
	}

	err = ioswitch.BindArrayVars(sw, ctx, o.Inputs)
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
	ioswitch.PutArrayVars(sw, o.Outputs)

	return sem.Acquire(ctx, int64(len(o.Outputs)))
}

type ECReconstruct struct {
	EC                cdssdk.ECRedundancy   `json:"ec"`
	Inputs            []*ioswitch.StreamVar `json:"inputs"`
	Outputs           []*ioswitch.StreamVar `json:"outputs"`
	InputBlockIndexes []int                 `json:"inputBlockIndexes"`
}

func (o *ECReconstruct) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	rs, err := ec.NewStreamRs(o.EC.K, o.EC.N, o.EC.ChunkSize)
	if err != nil {
		return fmt.Errorf("new ec: %w", err)
	}

	err = ioswitch.BindArrayVars(sw, ctx, o.Inputs)
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
	ioswitch.PutArrayVars(sw, o.Outputs)

	return sem.Acquire(ctx, int64(len(o.Outputs)))
}

type ECMultiply struct {
	Coef      [][]byte              `json:"coef"`
	Inputs    []*ioswitch.StreamVar `json:"inputs"`
	Outputs   []*ioswitch.StreamVar `json:"outputs"`
	ChunkSize int                   `json:"chunkSize"`
}

func (o *ECMultiply) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := ioswitch.BindArrayVars(sw, ctx, o.Inputs)
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
			err := sync2.ParallelDo(o.Inputs, func(s *ioswitch.StreamVar, i int) error {
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

	ioswitch.PutArrayVars(sw, o.Outputs)
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

func init() {
	OpUnion.AddT((*ECReconstructAny)(nil))
	OpUnion.AddT((*ECReconstruct)(nil))
	OpUnion.AddT((*ECMultiply)(nil))
}
