package event

import (
	"io/fs"
	"io/ioutil"
	"path/filepath"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/agent/internal/config"
	"gitlink.org.cn/cloudream/common/consts"
	evcst "gitlink.org.cn/cloudream/common/consts/event"
	"gitlink.org.cn/cloudream/common/utils"
	"gitlink.org.cn/cloudream/common/utils/logger"
	"gitlink.org.cn/cloudream/db/model"
	agtevt "gitlink.org.cn/cloudream/rabbitmq/message/agent/event"
	scmsg "gitlink.org.cn/cloudream/rabbitmq/message/scanner"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
)

type CheckStorage struct {
	agtevt.CheckStorage
}

func NewCheckStorage(dir string, isComplete bool, objects []model.StorageObject) *CheckStorage {
	return &CheckStorage{
		CheckStorage: agtevt.NewCheckStorage(dir, isComplete, objects),
	}
}

func (t *CheckStorage) TryMerge(other Event) bool {
	event, ok := other.(*CheckStorage)
	if !ok {
		return false
	}

	if event.IsComplete {
		t.IsComplete = true
		t.Objects = event.Objects
		return true
	}

	if !t.IsComplete {
		t.Objects = append(t.Objects, event.Objects...)
		t.Objects = lo.UniqBy(t.Objects, func(obj model.StorageObject) int { return obj.ObjectID })
		return true
	}

	return false
}

func (t *CheckStorage) Execute(execCtx ExecuteContext) {
	dirFullPath := filepath.Join(config.Cfg().StorageBaseDir, t.Directory)

	infos, err := ioutil.ReadDir(dirFullPath)
	if err != nil {
		logger.Warnf("list storage directory failed, err: %s", err.Error())

		execCtx.Args.Scanner.PostEvent(scmsg.NewPostEventBody(scevt.NewUpdateStorage(
			err.Error(),
			nil,
		), execCtx.Option.IsEmergency, execCtx.Option.DontMerge))
		return
	}

	fileInfos := lo.Filter(infos, func(info fs.FileInfo, index int) bool { return !info.IsDir() })

	if t.IsComplete {
		t.checkComplete(fileInfos, execCtx)
	} else {
		t.checkIncrement(fileInfos, execCtx)
	}
}

func (t *CheckStorage) checkIncrement(fileInfos []fs.FileInfo, execCtx ExecuteContext) {
	infosMap := make(map[string]fs.FileInfo)
	for _, info := range fileInfos {
		infosMap[info.Name()] = info
	}

	var updateStorageOps []scevt.UpdateStorageEntry
	for _, obj := range t.Objects {
		fileName := utils.MakeMoveOperationFileName(obj.ObjectID, obj.UserID)
		_, ok := infosMap[fileName]

		if ok {
			// 不需要做处理
			// 删除map中的记录，表示此记录已被检查过
			delete(infosMap, fileName)

		} else {
			// 只要文件不存在，就删除StorageObject表中的记录
			updateStorageOps = append(updateStorageOps, scevt.NewUpdateStorageEntry(obj.ObjectID, obj.UserID, evcst.UPDATE_STORAGE_DELETE))
		}
	}

	// 增量情况下，不需要对infosMap中没检查的记录进行处理

	execCtx.Args.Scanner.PostEvent(scmsg.NewPostEventBody(
		scevt.NewUpdateStorage(consts.STORAGE_DIRECTORY_STATUS_OK, updateStorageOps),
		execCtx.Option.IsEmergency,
		execCtx.Option.DontMerge,
	))
}

func (t *CheckStorage) checkComplete(fileInfos []fs.FileInfo, execCtx ExecuteContext) {
	infosMap := make(map[string]fs.FileInfo)
	for _, info := range fileInfos {
		infosMap[info.Name()] = info
	}

	var updateStorageOps []scevt.UpdateStorageEntry
	for _, obj := range t.Objects {
		fileName := utils.MakeMoveOperationFileName(obj.ObjectID, obj.UserID)
		_, ok := infosMap[fileName]

		if ok {
			// 不需要做处理
			// 删除map中的记录，表示此记录已被检查过
			delete(infosMap, fileName)

		} else {
			// 只要文件不存在，就删除StorageObject表中的记录
			updateStorageOps = append(updateStorageOps, scevt.NewUpdateStorageEntry(obj.ObjectID, obj.UserID, evcst.UPDATE_STORAGE_DELETE))
		}
	}

	// Storage中多出来的文件不做处理

	execCtx.Args.Scanner.PostEvent(scmsg.NewPostEventBody(
		scevt.NewUpdateStorage(consts.STORAGE_DIRECTORY_STATUS_OK, updateStorageOps),
		execCtx.Option.IsEmergency,
		execCtx.Option.DontMerge,
	))
}

func init() {
	Register(func(val agtevt.CheckStorage) Event {
		return NewCheckStorage(val.Directory, val.IsComplete, val.Objects)
	})
}
