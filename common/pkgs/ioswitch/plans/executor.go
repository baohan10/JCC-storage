package plans

import (
	"context"
	"fmt"
	"io"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type Executor struct {
	planID     ioswitch.PlanID
	planBlder  *PlanBuilder
	callback   *future.SetVoidFuture
	ctx        context.Context
	cancel     context.CancelFunc
	executorSw *ioswitch.Switch
}

func (e *Executor) BeginWrite(str io.ReadCloser, handle *ExecutorWriteStream) {
	handle.Var.Stream = str
	e.executorSw.PutVars(handle.Var)
}

func (e *Executor) BeginRead(handle *ExecutorReadStream) (io.ReadCloser, error) {
	err := e.executorSw.BindVars(e.ctx, handle.Var)
	if err != nil {
		return nil, fmt.Errorf("bind vars: %w", err)
	}

	return handle.Var.Stream, nil
}

func (e *Executor) Signal(signal *ExecutorSignalVar) {
	e.executorSw.PutVars(signal.Var)
}

func (e *Executor) Wait(ctx context.Context) (map[string]any, error) {
	err := e.callback.Wait(ctx)
	if err != nil {
		return nil, err
	}

	ret := make(map[string]any)
	e.planBlder.ExecutorPlan.StoreMap.Range(func(k, v any) bool {
		ret[k.(string)] = v
		return true
	})

	return ret, nil
}

func (e *Executor) execute() {
	wg := sync.WaitGroup{}

	for _, p := range e.planBlder.AgentPlans {
		wg.Add(1)

		go func(p *AgentPlanBuilder) {
			defer wg.Done()

			plan := ioswitch.Plan{
				ID:  e.planID,
				Ops: p.Ops,
			}

			cli, err := stgglb.AgentRPCPool.Acquire(stgglb.SelectGRPCAddress(&p.Node))
			if err != nil {
				e.stopWith(fmt.Errorf("new agent rpc client of node %v: %w", p.Node.NodeID, err))
				return
			}
			defer stgglb.AgentRPCPool.Release(cli)

			err = cli.ExecuteIOPlan(e.ctx, plan)
			if err != nil {
				e.stopWith(fmt.Errorf("execute plan at %v: %w", p.Node.NodeID, err))
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

	e.callback.SetVoid()
}

func (e *Executor) stopWith(err error) {
	e.callback.SetError(err)
	e.cancel()
}

type ExecutorWriteStream struct {
	Var *ioswitch.StreamVar
}

type ExecutorReadStream struct {
	Var *ioswitch.StreamVar
}

type ExecutorSignalVar struct {
	Var *ioswitch.SignalVar
}
