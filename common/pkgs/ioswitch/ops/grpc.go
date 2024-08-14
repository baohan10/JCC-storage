package ops

import (
	"context"
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type SendStream struct {
	Input *ioswitch.StreamVar `json:"input"`
	Send  *ioswitch.StreamVar `json:"send"`
	Node  cdssdk.Node         `json:"node"`
}

func (o *SendStream) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, o.Input)
	if err != nil {
		return err
	}
	defer o.Input.Stream.Close()

	agtCli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(&o.Node))
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	logger.Debugf("sending stream %v as %v to node %v", o.Input.ID, o.Send.ID, o.Node)

	// 发送后流的ID不同
	err = agtCli.SendStream(ctx, sw.Plan().ID, o.Send.ID, o.Input.Stream)
	if err != nil {
		return fmt.Errorf("sending stream: %w", err)
	}

	return nil
}

type GetStream struct {
	Signal *ioswitch.SignalVar `json:"signal"`
	Target *ioswitch.StreamVar `json:"target"`
	Output *ioswitch.StreamVar `json:"output"`
	Node   cdssdk.Node         `json:"node"`
}

func (o *GetStream) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	agtCli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(&o.Node))
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	logger.Debugf("getting stream %v as %v from node %v", o.Target.ID, o.Output.ID, o.Node)

	str, err := agtCli.GetStream(sw.Plan().ID, o.Target.ID, o.Signal)
	if err != nil {
		return fmt.Errorf("getting stream: %w", err)
	}

	fut := future.NewSetVoid()
	// 获取后送到本地的流ID是不同的
	o.Output.Stream = io2.AfterReadClosedOnce(str, func(closer io.ReadCloser) {
		fut.SetVoid()
	})
	sw.PutVars(o.Output)

	return fut.Wait(ctx)
}

type SendVar struct {
	Input ioswitch.Var `json:"input"`
	Send  ioswitch.Var `json:"send"`
	Node  cdssdk.Node  `json:"node"`
}

func (o *SendVar) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, o.Input)
	if err != nil {
		return err
	}

	agtCli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(&o.Node))
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	logger.Debugf("sending var %v as %v to node %v", o.Input.GetID(), o.Send.GetID(), o.Node)

	ioswitch.AssignVar(o.Input, o.Send)
	err = agtCli.SendVar(ctx, sw.Plan().ID, o.Send)
	if err != nil {
		return fmt.Errorf("sending var: %w", err)
	}

	return nil
}

type GetVar struct {
	Signal *ioswitch.SignalVar `json:"signal"`
	Target ioswitch.Var        `json:"target"`
	Output ioswitch.Var        `json:"output"`
	Node   cdssdk.Node         `json:"node"`
}

func (o *GetVar) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	agtCli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(&o.Node))
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	logger.Debugf("getting var %v as %v from node %v", o.Target.GetID(), o.Output.GetID(), o.Node)

	v2, err := agtCli.GetVar(ctx, sw.Plan().ID, o.Target, o.Signal)
	if err != nil {
		return fmt.Errorf("getting var: %w", err)
	}
	ioswitch.AssignVar(v2, o.Output)
	sw.PutVars(o.Output)

	return nil
}

func init() {
	OpUnion.AddT((*SendStream)(nil))
	OpUnion.AddT((*GetStream)(nil))
	OpUnion.AddT((*SendVar)(nil))
	OpUnion.AddT((*GetVar)(nil))
}
