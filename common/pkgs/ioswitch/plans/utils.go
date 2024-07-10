package plans

import (
	"github.com/google/uuid"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

func genRandomPlanID() ioswitch.PlanID {
	return ioswitch.PlanID(uuid.NewString())
}
