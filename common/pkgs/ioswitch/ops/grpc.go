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
	Stream *ioswitch.StreamVar `json:"stream"`
	Node   cdssdk.Node         `json:"node"`
}

func (o *SendStream) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, o.Stream)
	if err != nil {
		return err
	}
	defer o.Stream.Stream.Close()

	agtCli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(&o.Node))
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	logger.Debugf("sending stream %v to node %v", o.Stream.ID, o.Node)

	err = agtCli.SendStream(ctx, sw.Plan().ID, o.Stream.ID, o.Stream.Stream)
	if err != nil {
		return fmt.Errorf("sending stream: %w", err)
	}

	return nil
}

type GetStream struct {
	Stream *ioswitch.StreamVar `json:"stream"`
	Node   cdssdk.Node         `json:"node"`
}

func (o *GetStream) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	agtCli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(&o.Node))
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	logger.Debugf("getting stream %v from node %v", o.Stream.ID, o.Node)

	str, err := agtCli.GetStream(sw.Plan().ID, o.Stream.ID)
	if err != nil {
		return fmt.Errorf("getting stream: %w", err)
	}

	fut := future.NewSetVoid()
	o.Stream.Stream = io2.AfterReadClosedOnce(str, func(closer io.ReadCloser) {
		fut.SetVoid()
	})
	sw.PutVars(o.Stream)

	return fut.Wait(ctx)
}

type SendVar struct {
	Var  ioswitch.Var `json:"var"`
	Node cdssdk.Node  `json:"node"`
}

func (o *SendVar) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, o.Var)
	if err != nil {
		return err
	}

	agtCli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(&o.Node))
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	logger.Debugf("sending var %v to node %v", o.Var.GetID(), o.Node)

	err = agtCli.SendVar(ctx, sw.Plan().ID, o.Var)
	if err != nil {
		return fmt.Errorf("sending var: %w", err)
	}

	return nil
}

type GetVar struct {
	Var  ioswitch.Var `json:"var"`
	Node cdssdk.Node  `json:"node"`
}

func (o *GetVar) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	agtCli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(&o.Node))
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	logger.Debugf("getting var %v from node %v", o.Var.GetID(), o.Node)

	v2, err := agtCli.GetVar(ctx, sw.Plan().ID, o.Var)
	if err != nil {
		return fmt.Errorf("getting var: %w", err)
	}
	o.Var = v2
	sw.PutVars(o.Var)

	return nil
}

func init() {
	OpUnion.AddT((*SendStream)(nil))
	OpUnion.AddT((*GetStream)(nil))
	OpUnion.AddT((*SendVar)(nil))
	OpUnion.AddT((*GetVar)(nil))
}
