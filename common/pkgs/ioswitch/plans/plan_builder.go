package plans

import (
	"context"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type PlanBuilder struct {
	vars         []ioswitch.Var
	agentPlans   map[cdssdk.NodeID]*AgentPlanBuilder
	executorPlan ExecutorPlanBuilder
	storeMap     *sync.Map
}

func NewPlanBuilder() *PlanBuilder {
	bld := &PlanBuilder{
		agentPlans: make(map[cdssdk.NodeID]*AgentPlanBuilder),
		storeMap:   &sync.Map{},
	}
	bld.executorPlan.blder = bld

	return bld
}

func (b *PlanBuilder) AtExecutor() *ExecutorPlanBuilder {
	return &b.executorPlan
}

func (b *PlanBuilder) AtAgent(node cdssdk.Node) *AgentPlanBuilder {
	agtPlan, ok := b.agentPlans[node.NodeID]
	if !ok {
		agtPlan = &AgentPlanBuilder{
			blder: b,
			node:  node,
		}
		b.agentPlans[node.NodeID] = agtPlan
	}

	return agtPlan
}

func (b *PlanBuilder) Execute() *Executor {
	ctx, cancel := context.WithCancel(context.Background())
	planID := genRandomPlanID()

	execPlan := ioswitch.Plan{
		ID:  planID,
		Ops: b.executorPlan.ops,
	}

	exec := Executor{
		planID:     planID,
		plan:       b,
		callback:   future.NewSetVoid(),
		ctx:        ctx,
		cancel:     cancel,
		executorSw: ioswitch.NewSwitch(execPlan),
	}
	go exec.execute()

	return &exec
}

func (b *PlanBuilder) newStreamVar() *ioswitch.StreamVar {
	v := &ioswitch.StreamVar{
		ID: ioswitch.VarID(len(b.vars)),
	}
	b.vars = append(b.vars, v)

	return v
}

func (b *PlanBuilder) newIntVar() *ioswitch.IntVar {
	v := &ioswitch.IntVar{
		ID: ioswitch.VarID(len(b.vars)),
	}
	b.vars = append(b.vars, v)

	return v
}

func (b *PlanBuilder) newStringVar() *ioswitch.StringVar {
	v := &ioswitch.StringVar{
		ID: ioswitch.VarID(len(b.vars)),
	}
	b.vars = append(b.vars, v)

	return v
}
