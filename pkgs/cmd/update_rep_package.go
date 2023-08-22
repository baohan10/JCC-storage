package cmd

import (
	"fmt"

	"github.com/samber/lo"
	mysort "gitlink.org.cn/cloudream/common/utils/sort"
	"gitlink.org.cn/cloudream/storage-common/pkgs/distlock/reqbuilder"

	"gitlink.org.cn/cloudream/storage-common/globals"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage-common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
)

type UpdateRepPackage struct {
	userID     int64
	packageID  int64
	objectIter iterator.UploadingObjectIterator
}

type UpdateNodeInfo struct {
	UploadNodeInfo
	HasOldObject bool
}

type UpdateRepPackageResult struct {
	ObjectResults []RepObjectUploadResult
}

func NewUpdateRepPackage(userID int64, packageID int64, objectIter iterator.UploadingObjectIterator) *UpdateRepPackage {
	return &UpdateRepPackage{
		userID:     userID,
		packageID:  packageID,
		objectIter: objectIter,
	}
}

func (t *UpdateRepPackage) Execute(ctx *UpdatePackageContext) (*UpdateRepPackageResult, error) {
	defer t.objectIter.Close()

	coorCli, err := globals.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}

	reqBlder := reqbuilder.NewBuilder()
	// 如果本地的IPFS也是存储系统的一个节点，那么从本地上传时，需要加锁
	if globals.Local.NodeID != nil {
		reqBlder.IPFS().CreateAnyRep(*globals.Local.NodeID)
	}
	mutex, err := reqBlder.
		Metadata().
		// 用于查询可用的上传节点
		Node().ReadAny().
		// 用于创建包信息
		Package().WriteOne(t.packageID).
		// 用于创建包中的文件的信息
		Object().CreateAny().
		// 用于设置EC配置
		ObjectBlock().CreateAny().
		// 用于创建Cache记录
		Cache().CreateAny().
		MutexLock(ctx.Distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()
	getUserNodesResp, err := coorCli.GetUserNodes(coormq.NewGetUserNodes(t.userID))
	if err != nil {
		return nil, fmt.Errorf("getting user nodes: %w", err)
	}

	findCliLocResp, err := coorCli.FindClientLocation(coormq.NewFindClientLocation(globals.Local.ExternalIP))
	if err != nil {
		return nil, fmt.Errorf("finding client location: %w", err)
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
	ipfsMutex, err := reqbuilder.NewBuilder().
		IPFS().CreateAnyRep(uploadNode.Node.NodeID).
		MutexLock(ctx.Distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer ipfsMutex.Unlock()

	rets, err := uploadAndUpdateRepPackage(t.packageID, t.objectIter, uploadNode.UploadNodeInfo)
	if err != nil {
		return nil, err
	}

	return &UpdateRepPackageResult{
		ObjectResults: rets,
	}, nil
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
