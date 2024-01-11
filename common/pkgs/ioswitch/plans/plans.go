package plans

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type AgentPlan struct {
	Node cdssdk.Node
	Plan ioswitch.Plan
}

type ComposedPlan struct {
	ID         ioswitch.PlanID
	AgentPlans []AgentPlan
}
