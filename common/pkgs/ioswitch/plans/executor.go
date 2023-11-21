package plans

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	myio "gitlink.org.cn/cloudream/common/utils/io"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
)

type ExecutorResult struct {
	ResultValues map[string]any
}

type Executor struct {
	plan        ComposedPlan
	callback    *future.SetValueFuture[ExecutorResult]
	mqClis      []*agtmq.Client
	planTaskIDs []string
}

func Execute(plan ComposedPlan) (*Executor, error) {
	executor := Executor{
		plan:     plan,
		callback: future.NewSetValue[ExecutorResult](),
	}

	var err error
	for _, a := range plan.AgentPlans {
		var cli *agtmq.Client
		cli, err = stgglb.AgentMQPool.Acquire(a.Node.NodeID)
		if err != nil {
			executor.Close()
			return nil, fmt.Errorf("new mq client for %d: %w", a.Node.NodeID, err)
		}

		executor.mqClis = append(executor.mqClis, cli)
	}

	for i, a := range plan.AgentPlans {
		cli := executor.mqClis[i]

		_, err := cli.SetupIOPlan(agtmq.NewSetupIOPlan(a.Plan))
		if err != nil {
			for i -= 1; i >= 0; i-- {
				executor.mqClis[i].CancelIOPlan(agtmq.NewCancelIOPlan(plan.ID))
			}
			executor.Close()
			return nil, fmt.Errorf("setup plan at %d: %w", a.Node.NodeID, err)
		}
	}

	for i, a := range plan.AgentPlans {
		cli := executor.mqClis[i]

		resp, err := cli.StartIOPlan(agtmq.NewStartIOPlan(a.Plan.ID))
		if err != nil {
			executor.cancelAll()
			executor.Close()
			return nil, fmt.Errorf("setup plan at %d: %w", a.Node.NodeID, err)
		}

		executor.planTaskIDs = append(executor.planTaskIDs, resp.TaskID)
	}

	go executor.pollResult()

	return &executor, nil
}

func (e *Executor) SendStream(info *FromExecutorStream, stream io.Reader) error {
	// TODO 根据地域选择IP
	agtCli, err := stgglb.AgentRPCPool.Acquire(info.toNode.ExternalIP, info.toNode.ExternalGRPCPort)
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	return agtCli.SendStream(e.plan.ID, info.info.ID, stream)
}

func (e *Executor) ReadStream(info *ToExecutorStream) (io.ReadCloser, error) {
	// TODO 根据地域选择IP
	agtCli, err := stgglb.AgentRPCPool.Acquire(info.fromNode.ExternalIP, info.fromNode.ExternalGRPCPort)
	if err != nil {
		return nil, fmt.Errorf("new agent rpc client: %w", err)
	}

	str, err := agtCli.FetchStream(e.plan.ID, info.info.ID)
	if err != nil {
		return nil, err
	}

	return myio.AfterReadClosed(str, func(closer io.ReadCloser) {
		stgglb.AgentRPCPool.Release(agtCli)
	}), nil
}

func (e *Executor) cancelAll() {
	for _, cli := range e.mqClis {
		cli.CancelIOPlan(agtmq.NewCancelIOPlan(e.plan.ID))
	}
}

func (e *Executor) Close() {
	for _, c := range e.mqClis {
		stgglb.AgentMQPool.Release(c)
	}
}

func (e *Executor) pollResult() {
	wg := sync.WaitGroup{}
	anyErr := atomic.Value{}
	anyErr.Store(nil)
	rets := make([]*ioswitch.PlanResult, len(e.plan.AgentPlans))

	for i, id := range e.planTaskIDs {
		idx := i
		taskID := id

		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				resp, err := e.mqClis[idx].WaitIOPlan(agtmq.NewWaitIOPlan(taskID, 5000))
				if err != nil {
					anyErr.Store(err)
					break
				}

				if resp.IsComplete {
					if resp.Error != "" {
						anyErr.Store(errors.New(resp.Error))
					} else {
						rets[idx] = &resp.Result
					}
					break
				}

				if anyErr.Load() != nil {
					break
				}
			}
		}()
	}

	wg.Wait()

	err := anyErr.Load().(error)
	if err != nil {
		e.callback.SetError(err)
		return
	}

	reducedRet := ExecutorResult{
		ResultValues: make(map[string]any),
	}
	for _, ret := range rets {
		for k, v := range ret.Values {
			reducedRet.ResultValues[k] = v
		}
	}

	e.callback.SetValue(reducedRet)
}
