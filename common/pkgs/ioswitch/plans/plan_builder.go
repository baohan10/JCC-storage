package plans

import (
	"fmt"

	"github.com/google/uuid"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type StreamInfo struct {
	ID ioswitch.StreamID
}

type PlanBuilder struct {
	streams    []*StreamInfo
	agentPlans map[cdssdk.NodeID]*AgentPlanBuilder
}

func (b *PlanBuilder) Build() (*ComposedPlan, error) {
	planID := uuid.NewString()

	var agentPlans []AgentPlan
	for _, b := range b.agentPlans {
		plan, err := b.Build(ioswitch.PlanID(planID))
		if err != nil {
			return nil, err
		}

		agentPlans = append(agentPlans, plan)
	}

	return &ComposedPlan{
		ID:         ioswitch.PlanID(planID),
		AgentPlans: agentPlans,
	}, nil
}

func (b *PlanBuilder) newStream() *StreamInfo {
	str := &StreamInfo{
		ID: ioswitch.StreamID(fmt.Sprintf("%d", len(b.streams)+1)),
	}

	b.streams = append(b.streams, str)

	return str
}

func NewPlanBuilder() PlanBuilder {
	return PlanBuilder{
		agentPlans: make(map[cdssdk.NodeID]*AgentPlanBuilder),
	}
}

func (b *PlanBuilder) FromExecutor() *FromExecutorStream {
	return &FromExecutorStream{
		owner: b,
		info:  b.newStream(),
	}
}

func (b *PlanBuilder) AtAgent(node model.Node) *AgentPlanBuilder {
	agtPlan, ok := b.agentPlans[node.NodeID]
	if !ok {
		agtPlan = &AgentPlanBuilder{
			owner: b,
			node:  node,
		}
		b.agentPlans[node.NodeID] = agtPlan
	}

	return agtPlan
}

type FromExecutorStream struct {
	owner  *PlanBuilder
	info   *StreamInfo
	toNode *model.Node
}

func (s *FromExecutorStream) ToNode(node model.Node) *AgentStream {
	s.toNode = &node
	return &AgentStream{
		owner: s.owner.AtAgent(node),
		info:  s.info,
	}
}

type ToExecutorStream struct {
	info     *StreamInfo
	fromNode *model.Node
}

type MultiStream struct {
	Streams []*AgentStream
}

func (m *MultiStream) Count() int {
	return len(m.Streams)
}

func (m *MultiStream) Stream(index int) *AgentStream {
	return m.Streams[index]
}
