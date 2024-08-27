package ioswitch2

import (
	"context"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/types"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/serder"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
)

var _ = serder.UseTypeUnionExternallyTagged(types.Ref(types.NewTypeUnion[exec.WorkerInfo](
	(*AgentWorker)(nil),
)))

type AgentWorker struct {
	Node cdssdk.Node
}

func (w *AgentWorker) NewClient() (exec.WorkerClient, error) {
	cli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(&w.Node))
	if err != nil {
		return nil, err
	}

	return &AgentWorkerClient{cli: cli}, nil
}

func (w *AgentWorker) String() string {
	return w.Node.String()
}

func (w *AgentWorker) Equals(worker exec.WorkerInfo) bool {
	aw, ok := worker.(*AgentWorker)
	if !ok {
		return false
	}

	return w.Node.NodeID == aw.Node.NodeID
}

type AgentWorkerClient struct {
	cli *agtrpc.PoolClient
}

func (c *AgentWorkerClient) ExecutePlan(ctx context.Context, plan exec.Plan) error {
	return c.cli.ExecuteIOPlan(ctx, plan)
}
func (c *AgentWorkerClient) SendStream(ctx context.Context, planID exec.PlanID, v *exec.StreamVar, str io.ReadCloser) error {
	return c.cli.SendStream(ctx, planID, v.ID, str)
}
func (c *AgentWorkerClient) SendVar(ctx context.Context, planID exec.PlanID, v exec.Var) error {
	return c.cli.SendVar(ctx, planID, v)
}
func (c *AgentWorkerClient) GetStream(ctx context.Context, planID exec.PlanID, v *exec.StreamVar, signal *exec.SignalVar) (io.ReadCloser, error) {
	return c.cli.GetStream(ctx, planID, v.ID, signal)
}
func (c *AgentWorkerClient) GetVar(ctx context.Context, planID exec.PlanID, v exec.Var, signal *exec.SignalVar) error {
	return c.cli.GetVar(ctx, planID, v, signal)
}
func (c *AgentWorkerClient) Close() error {
	stgglb.AgentRPCPool.Release(c.cli)
	return nil
}
