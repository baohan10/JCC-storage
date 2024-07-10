package ops

import (
	"context"
	"fmt"
	"io"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
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

func init() {
	OpUnion.AddT((*ECReconstructAny)(nil))
	OpUnion.AddT((*ECReconstruct)(nil))
}
