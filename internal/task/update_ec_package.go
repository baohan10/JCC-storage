package task

import (
	"time"

	"gitlink.org.cn/cloudream/storage-client/internal/config"
	"gitlink.org.cn/cloudream/storage-common/pkgs/cmd"
	"gitlink.org.cn/cloudream/storage-common/pkgs/iterator"
)

type UpdateECPackageResult = cmd.UpdateECPackageResult

type UpdateECPackage struct {
	cmd cmd.UpdateECPackage

	Result *UpdateECPackageResult
}

func NewUpdateECPackage(userID int64, packageID int64, objectIter iterator.UploadingObjectIterator) *UpdateECPackage {
	return &UpdateECPackage{
		cmd: *cmd.NewUpdateECPackage(userID, packageID, objectIter),
	}
}

func (t *UpdateECPackage) Execute(ctx TaskContext, complete CompleteFn) {
	ret, err := t.cmd.Execute(&cmd.UpdateECPackageContext{
		UpdatePackageContext: &cmd.UpdatePackageContext{
			Distlock: ctx.distlock,
		},
		ECPacketSize: config.Cfg().ECPacketSize,
	})

	t.Result = ret

	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}
