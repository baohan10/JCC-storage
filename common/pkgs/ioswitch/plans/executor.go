package plans

import (
	"context"
	"fmt"
	"io"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/ops"
)

type Executor struct {
	planID     ioswitch.PlanID
	plan       *PlanBuilder
	callback   *future.SetVoidFuture
	ctx        context.Context
	cancel     context.CancelFunc
	executorSw *ioswitch.Switch
}

func (e *Executor) BeginWrite(str io.ReadCloser, target ExecutorWriteStream) {
	target.stream.Stream = str
	e.executorSw.PutVars(target.stream)
}

func (e *Executor) BeginRead(target ExecutorReadStream) (io.ReadCloser, error) {
	err := e.executorSw.BindVars(e.ctx, target.stream)
	if err != nil {
		return nil, fmt.Errorf("bind vars: %w", err)
	}

	return target.stream.Stream, nil
}

func (e *Executor) Wait(ctx context.Context) (map[string]any, error) {
	err := e.callback.Wait(ctx)
	if err != nil {
		return nil, err
	}

	ret := make(map[string]any)
	e.plan.storeMap.Range(func(k, v any) bool {
		ret[k.(string)] = v
		return true
	})

	return ret, nil
}

func (e *Executor) execute() {
	wg := sync.WaitGroup{}

	for _, p := range e.plan.agentPlans {
		wg.Add(1)

		go func(p *AgentPlanBuilder) {
			defer wg.Done()

			plan := ioswitch.Plan{
				ID:  e.planID,
				Ops: p.ops,
			}

			cli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(&p.node))
			if err != nil {
				e.stopWith(fmt.Errorf("new agent rpc client of node %v: %w", p.node.NodeID, err))
				return
			}
			defer stgglb.AgentRPCPool.Release(cli)

			err = cli.ExecuteIOPlan(e.ctx, plan)
			if err != nil {
				e.stopWith(fmt.Errorf("execute plan at %v: %w", p.node.NodeID, err))
				return
			}
		}(p)
	}

	err := e.executorSw.Run(e.ctx)
	if err != nil {
		e.stopWith(fmt.Errorf("run executor switch: %w", err))
		return
	}

	wg.Wait()
}

func (e *Executor) stopWith(err error) {
	e.callback.SetError(err)
	e.cancel()
}

type ExecutorPlanBuilder struct {
	blder *PlanBuilder
	ops   []ioswitch.Op
}

type ExecutorStreamVar struct {
	blder *PlanBuilder
	v     *ioswitch.StreamVar
}

type ExecutorStringVar struct {
	blder *PlanBuilder
	v     *ioswitch.StringVar
}

type ExecutorWriteStream struct {
	stream *ioswitch.StreamVar
}

func (b *ExecutorPlanBuilder) WillWrite() (ExecutorWriteStream, *ExecutorStreamVar) {
	stream := b.blder.newStreamVar()
	return ExecutorWriteStream{stream}, &ExecutorStreamVar{blder: b.blder, v: stream}
}

type ExecutorReadStream struct {
	stream *ioswitch.StreamVar
}

func (v *ExecutorStreamVar) WillRead() ExecutorReadStream {
	return ExecutorReadStream{v.v}
}

func (s *ExecutorStringVar) Store(key string) {
	s.blder.executorPlan.ops = append(s.blder.executorPlan.ops, &ops.Store{
		Var:   s.v,
		Key:   key,
		Store: s.blder.storeMap,
	})
}

func (s *ExecutorStreamVar) To(node cdssdk.Node) *AgentStreamVar {
	s.blder.executorPlan.ops = append(s.blder.executorPlan.ops, &ops.SendStream{Stream: s.v, Node: node})
	return &AgentStreamVar{
		owner: s.blder.AtAgent(node),
		v:     s.v,
	}
}
