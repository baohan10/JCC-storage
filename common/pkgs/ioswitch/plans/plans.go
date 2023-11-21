package plans

import (
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type AgentPlan struct {
	Node model.Node
	Plan ioswitch.Plan
}

type ComposedPlan struct {
	ID         ioswitch.PlanID
	AgentPlans []AgentPlan
}
