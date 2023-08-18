package cmd

import (
	"fmt"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/distlock/reqbuilder"
	mysort "gitlink.org.cn/cloudream/common/utils/sort"

	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage-common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
)

type UpdateRepPackage struct {
	userID       int64
	packageID    int64
	objectIter   iterator.UploadingObjectIterator
	uploadConfig UploadConfig

	Result UpdateRepPackageResult
}

type UpdateNodeInfo struct {
	UploadNodeInfo
	HasOldObject bool
}

type UpdateRepPackageResult struct {
	ObjectResults []RepObjectUploadResult
}

func NewUpdateRepPackage(userID int64, packageID int64, objectIter iterator.UploadingObjectIterator, uploadConfig UploadConfig) *UpdateRepPackage {
	return &UpdateRepPackage{
		userID:       userID,
		packageID:    packageID,
		objectIter:   objectIter,
		uploadConfig: uploadConfig,
	}
}

func (t *UpdateRepPackage) Execute(ctx TaskContext, complete CompleteFn) {
	err := t.do(ctx)
	t.objectIter.Close()
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *UpdateRepPackage) do(ctx TaskContext) error {
	/*
		TODO2
		reqBlder := reqbuilder.NewBuilder()

		// 如果本地的IPFS也是存储系统的一个节点，那么从本地上传时，需要加锁
		if t.uploadConfig.LocalNodeID != nil {
			reqBlder.IPFS().CreateAnyRep(*t.uploadConfig.LocalNodeID)
		}

		// TODO2
		mutex, err := reqBlder.
			Metadata().
			// 用于判断用户是否有对象的权限
			UserBucket().ReadAny().
			// 用于读取、修改对象信息
			Object().WriteOne(t.objectID).
			// 用于更新Rep配置
			ObjectRep().WriteOne(t.objectID).
			// 用于查询可用的上传节点
			Node().ReadAny().
			// 用于创建Cache记录
			Cache().CreateAny().
			// 用于修改Move此Object的记录的状态
			StorageObject().WriteAny().
			MutexLock(ctx.DistLock())
		if err != nil {
			return fmt.Errorf("acquire locks failed, err: %w", err)
		}
		defer mutex.Unlock()
	*/
	getUserNodesResp, err := ctx.Coordinator().GetUserNodes(coormq.NewGetUserNodes(t.userID))
	if err != nil {
		return fmt.Errorf("getting user nodes: %w", err)
	}

	findCliLocResp, err := ctx.Coordinator().FindClientLocation(coormq.NewFindClientLocation(t.uploadConfig.ExternalIP))
	if err != nil {
		return fmt.Errorf("finding client location: %w", err)
	}

	nodeInfos := lo.Map(getUserNodesResp.Nodes, func(node model.Node, index int) UpdateNodeInfo {
		return UpdateNodeInfo{
			UploadNodeInfo: UploadNodeInfo{
				Node:           node,
				IsSameLocation: node.LocationID == findCliLocResp.Location.LocationID,
			},
		}
	})
	// 上传文件的方式优先级：
	// 1. 本地IPFS
	// 2. 包含了旧文件，且与客户端在同地域的节点
	// 3. 不在同地域，但包含了旧文件的节点
	// 4. 同地域节点
	// TODO 需要考虑在多文件的情况下的规则
	uploadNode := t.chooseUploadNode(nodeInfos)

	// 防止上传的副本被清除
	mutex2, err := reqbuilder.NewBuilder().
		IPFS().CreateAnyRep(uploadNode.Node.NodeID).
		MutexLock(ctx.DistLock())
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex2.Unlock()

	rets, err := uploadAndUpdateRepPackage(ctx, t.packageID, t.objectIter, uploadNode.UploadNodeInfo, t.uploadConfig)
	if err != nil {
		return err
	}

	t.Result.ObjectResults = rets
	return nil
}

// chooseUploadNode 选择一个上传文件的节点
// 1. 从与当前客户端相同地域的节点中随机选一个
// 2. 没有用的话从所有节点中随机选一个
func (t *UpdateRepPackage) chooseUploadNode(nodes []UpdateNodeInfo) UpdateNodeInfo {
	mysort.Sort(nodes, func(left, right UpdateNodeInfo) int {
		v := -mysort.CmpBool(left.HasOldObject, right.HasOldObject)
		if v != 0 {
			return v
		}

		return -mysort.CmpBool(left.IsSameLocation, right.IsSameLocation)
	})

	return nodes[0]
}
