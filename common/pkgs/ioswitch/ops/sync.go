package ops

import (
	"context"
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type OnStreamBegin struct {
	Raw    *ioswitch.StreamVar `json:"raw"`
	New    *ioswitch.StreamVar `json:"new"`
	Signal *ioswitch.SignalVar `json:"signal"`
}

func (o *OnStreamBegin) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, o.Raw)
	if err != nil {
		return err
	}

	o.New.Stream = o.Raw.Stream

	sw.PutVars(o.New, o.Signal)
	return nil
}

type OnStreamEnd struct {
	Raw    *ioswitch.StreamVar `json:"raw"`
	New    *ioswitch.StreamVar `json:"new"`
	Signal *ioswitch.SignalVar `json:"signal"`
}

type onStreamEnd struct {
	inner    io.ReadCloser
	callback *future.SetVoidFuture
}

func (o *onStreamEnd) Read(p []byte) (n int, err error) {
	n, err = o.inner.Read(p)
	if err == io.EOF {
		o.callback.SetVoid()
	} else if err != nil {
		o.callback.SetError(err)
	}
	return n, err
}

func (o *onStreamEnd) Close() error {
	o.callback.SetError(fmt.Errorf("stream closed early"))
	return o.inner.Close()
}

func (o *OnStreamEnd) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, o.Raw)
	if err != nil {
		return err
	}

	cb := future.NewSetVoid()

	o.New.Stream = &onStreamEnd{
		inner:    o.Raw.Stream,
		callback: cb,
	}
	sw.PutVars(o.New)

	err = cb.Wait(ctx)
	if err != nil {
		return err
	}

	sw.PutVars(o.Signal)
	return nil
}

type HoldUntil struct {
	Waits []*ioswitch.SignalVar `json:"waits"`
	Holds []ioswitch.Var        `json:"holds"`
	Emits []ioswitch.Var        `json:"emits"`
}

func (w *HoldUntil) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, w.Holds...)
	if err != nil {
		return err
	}

	err = ioswitch.BindArrayVars(sw, ctx, w.Waits)
	if err != nil {
		return err
	}

	for i := 0; i < len(w.Holds); i++ {
		err := ioswitch.AssignVar(w.Holds[i], w.Emits[i])
		if err != nil {
			return err
		}
	}

	sw.PutVars(w.Emits...)
	return nil
}

type HangUntil struct {
	Waits []*ioswitch.SignalVar `json:"waits"`
	Op    ioswitch.Op           `json:"op"`
}

func (h *HangUntil) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := ioswitch.BindArrayVars(sw, ctx, h.Waits)
	if err != nil {
		return err
	}

	return h.Op.Execute(ctx, sw)
}

type Broadcast struct {
	Source  *ioswitch.SignalVar   `json:"source"`
	Targets []*ioswitch.SignalVar `json:"targets"`
}

func (b *Broadcast) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, b.Source)
	if err != nil {
		return err
	}

	ioswitch.PutArrayVars(sw, b.Targets)
	return nil
}

func init() {
	OpUnion.AddT((*OnStreamBegin)(nil))
	OpUnion.AddT((*OnStreamEnd)(nil))
	OpUnion.AddT((*HoldUntil)(nil))
	OpUnion.AddT((*HangUntil)(nil))
	OpUnion.AddT((*Broadcast)(nil))
}
