package stgglb

import (
	stgmodels "gitlink.org.cn/cloudream/storage/common/models"
)

var Local *stgmodels.LocalMachineInfo

func InitLocal(info *stgmodels.LocalMachineInfo) {
	Local = info
}
