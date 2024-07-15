package ops

import (
	"context"
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	"golang.org/x/sync/semaphore"
)

type CloneStream struct {
	Input   *ioswitch.StreamVar   `json:"input"`
	Outputs []*ioswitch.StreamVar `json:"outputs"`
}

func (o *CloneStream) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, o.Input)
	if err != nil {
		return err
	}
	defer o.Input.Stream.Close()

	cloned := io2.Clone(o.Input.Stream, len(o.Outputs))

	sem := semaphore.NewWeighted(int64(len(o.Outputs)))
	for i, s := range cloned {
		sem.Acquire(ctx, 1)

		o.Outputs[i].Stream = io2.AfterReadClosedOnce(s, func(closer io.ReadCloser) {
			sem.Release(1)
		})
	}
	ioswitch.PutArrayVars(sw, o.Outputs)

	return sem.Acquire(ctx, int64(len(o.Outputs)))
}

type CloneVar struct {
	Raw     ioswitch.Var   `json:"raw"`
	Cloneds []ioswitch.Var `json:"cloneds"`
}

func (o *CloneVar) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, o.Raw)
	if err != nil {
		return err
	}

	for _, v := range o.Cloneds {
		if err := ioswitch.AssignVar(o.Raw, v); err != nil {
			return fmt.Errorf("clone var: %w", err)
		}
	}
	sw.PutVars(o.Cloneds...)

	return nil
}

func init() {
	OpUnion.AddT((*CloneStream)(nil))
	OpUnion.AddT((*CloneVar)(nil))
}
