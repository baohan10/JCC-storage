package exec

import (
	"context"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/lo2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type PlanBuilder struct {
	Vars         []ioswitch.Var
	AgentPlans   map[cdssdk.NodeID]*AgentPlanBuilder
	ExecutorPlan ExecutorPlanBuilder
}

func NewPlanBuilder() *PlanBuilder {
	bld := &PlanBuilder{
		AgentPlans: make(map[cdssdk.NodeID]*AgentPlanBuilder),
		ExecutorPlan: ExecutorPlanBuilder{
			StoreMap: &sync.Map{},
		},
	}

	return bld
}

func (b *PlanBuilder) AtExecutor() *ExecutorPlanBuilder {
	return &b.ExecutorPlan
}

func (b *PlanBuilder) AtAgent(node cdssdk.Node) *AgentPlanBuilder {
	agtPlan, ok := b.AgentPlans[node.NodeID]
	if !ok {
		agtPlan = &AgentPlanBuilder{
			Node: node,
		}
		b.AgentPlans[node.NodeID] = agtPlan
	}

	return agtPlan
}

func (b *PlanBuilder) NewStreamVar() *ioswitch.StreamVar {
	v := &ioswitch.StreamVar{
		ID: ioswitch.VarID(len(b.Vars)),
	}
	b.Vars = append(b.Vars, v)

	return v
}

func (b *PlanBuilder) NewIntVar() *ioswitch.IntVar {
	v := &ioswitch.IntVar{
		ID: ioswitch.VarID(len(b.Vars)),
	}
	b.Vars = append(b.Vars, v)

	return v
}

func (b *PlanBuilder) NewStringVar() *ioswitch.StringVar {
	v := &ioswitch.StringVar{
		ID: ioswitch.VarID(len(b.Vars)),
	}
	b.Vars = append(b.Vars, v)

	return v
}
func (b *PlanBuilder) NewSignalVar() *ioswitch.SignalVar {
	v := &ioswitch.SignalVar{
		ID: ioswitch.VarID(len(b.Vars)),
	}
	b.Vars = append(b.Vars, v)

	return v
}

func (b *PlanBuilder) Execute() *Executor {
	ctx, cancel := context.WithCancel(context.Background())
	planID := genRandomPlanID()

	execPlan := ioswitch.Plan{
		ID:  planID,
		Ops: b.ExecutorPlan.Ops,
	}

	exec := Executor{
		planID:     planID,
		planBlder:  b,
		callback:   future.NewSetVoid(),
		ctx:        ctx,
		cancel:     cancel,
		executorSw: ioswitch.NewSwitch(execPlan),
	}
	go exec.execute()

	return &exec
}

type AgentPlanBuilder struct {
	Node cdssdk.Node
	Ops  []ioswitch.Op
}

func (b *AgentPlanBuilder) AddOp(op ioswitch.Op) {
	b.Ops = append(b.Ops, op)
}

func (b *AgentPlanBuilder) RemoveOp(op ioswitch.Op) {
	b.Ops = lo2.Remove(b.Ops, op)
}

type ExecutorPlanBuilder struct {
	Ops      []ioswitch.Op
	StoreMap *sync.Map
}

func (b *ExecutorPlanBuilder) AddOp(op ioswitch.Op) {
	b.Ops = append(b.Ops, op)
}

func (b *ExecutorPlanBuilder) RemoveOp(op ioswitch.Op) {
	b.Ops = lo2.Remove(b.Ops, op)
}
