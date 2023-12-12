package cmd

import (
	"fmt"

	"github.com/samber/lo"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type UpdatePackage struct {
	userID     cdssdk.UserID
	packageID  cdssdk.PackageID
	objectIter iterator.UploadingObjectIterator
}

type UpdatePackageResult struct {
	ObjectResults []ObjectUploadResult
}

type UpdateNodeInfo struct {
	UploadNodeInfo
	HasOldObject bool
}

func NewUpdatePackage(userID cdssdk.UserID, packageID cdssdk.PackageID, objIter iterator.UploadingObjectIterator) *UpdatePackage {
	return &UpdatePackage{
		userID:     userID,
		packageID:  packageID,
		objectIter: objIter,
	}
}

func (t *UpdatePackage) Execute(ctx *UpdatePackageContext) (*UpdatePackageResult, error) {
	defer t.objectIter.Close()

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}

	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 用于查询可用的上传节点
		Node().ReadAny().
		// 用于修改包信息
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

	userNodes := lo.Map(getUserNodesResp.Nodes, func(node model.Node, index int) UploadNodeInfo {
		return UploadNodeInfo{
			Node:           node,
			IsSameLocation: node.LocationID == stgglb.Local.LocationID,
		}
	})

	// 给上传节点的IPFS加锁
	ipfsReqBlder := reqbuilder.NewBuilder()
	// 如果本地的IPFS也是存储系统的一个节点，那么从本地上传时，需要加锁
	if stgglb.Local.NodeID != nil {
		ipfsReqBlder.IPFS().CreateAnyRep(*stgglb.Local.NodeID)
	}
	for _, node := range userNodes {
		if stgglb.Local.NodeID != nil && node.Node.NodeID == *stgglb.Local.NodeID {
			continue
		}

		ipfsReqBlder.IPFS().CreateAnyRep(node.Node.NodeID)
	}
	// 防止上传的副本被清除
	ipfsMutex, err := ipfsReqBlder.MutexLock(ctx.Distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer ipfsMutex.Unlock()

	rets, err := uploadAndUpdatePackage(t.packageID, t.objectIter, userNodes, nil)
	if err != nil {
		return nil, err
	}

	return &UpdatePackageResult{
		ObjectResults: rets,
	}, nil
}