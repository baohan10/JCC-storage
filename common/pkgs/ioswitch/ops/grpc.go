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

type GRPCSend struct {
	LocalID  ioswitch.StreamID `json:"localID"`
	RemoteID ioswitch.StreamID `json:"remoteID"`
	Node     cdssdk.Node       `json:"node"`
}

func (o *GRPCSend) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	logger.
		WithField("LocalID", o.LocalID).
		WithField("RemoteID", o.RemoteID).
		Debugf("grpc send")

	strs, err := sw.WaitStreams(planID, o.LocalID)
	if err != nil {
		return err
	}
	defer strs[0].Stream.Close()

	// TODO 根据客户端地址选择IP和端口
	agtCli, err := stgglb.AgentRPCPool.Acquire(o.Node.ExternalIP, o.Node.ExternalGRPCPort)
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	err = agtCli.SendStream(planID, o.RemoteID, strs[0].Stream)
	if err != nil {
		return fmt.Errorf("sending stream: %w", err)
	}

	return nil
}

type GRPCFetch struct {
	RemoteID ioswitch.StreamID `json:"remoteID"`
	LocalID  ioswitch.StreamID `json:"localID"`
	Node     cdssdk.Node       `json:"node"`
}

func (o *GRPCFetch) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	// TODO 根据客户端地址选择IP和端口
	agtCli, err := stgglb.AgentRPCPool.Acquire(o.Node.ExternalIP, o.Node.ExternalGRPCPort)
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	str, err := agtCli.FetchStream(planID, o.RemoteID)
	if err != nil {
		return fmt.Errorf("fetching stream: %w", err)
	}

	fut := future.NewSetVoid()
	str = io2.AfterReadClosedOnce(str, func(closer io.ReadCloser) {
		fut.SetVoid()
	})

	sw.StreamReady(planID, ioswitch.NewStream(o.LocalID, str))

	// TODO
	fut.Wait(context.TODO())

	return err
}

func init() {
	OpUnion.AddT((*GRPCSend)(nil))
	OpUnion.AddT((*GRPCFetch)(nil))
}
