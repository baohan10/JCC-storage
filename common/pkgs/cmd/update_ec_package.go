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

type UpdateECPackage struct {
	userID     int64
	packageID  int64
	objectIter iterator.UploadingObjectIterator
}

type UpdateECPackageResult struct {
	ObjectResults []ECObjectUploadResult
}

func NewUpdateECPackage(userID int64, packageID int64, objIter iterator.UploadingObjectIterator) *UpdateECPackage {
	return &UpdateECPackage{
		userID:     userID,
		packageID:  packageID,
		objectIter: objIter,
	}
}

func (t *UpdateECPackage) Execute(ctx *UpdatePackageContext) (*UpdateECPackageResult, error) {
	defer t.objectIter.Close()

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}

	mutex, err := reqbuilder.NewBuilder().
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

	getPkgResp, err := coorCli.GetPackage(coormq.NewGetPackage(t.userID, t.packageID))
	if err != nil {
		return nil, fmt.Errorf("getting package: %w", err)
	}

	getUserNodesResp, err := coorCli.GetUserNodes(coormq.NewGetUserNodes(t.userID))
	if err != nil {
		return nil, fmt.Errorf("getting user nodes: %w", err)
	}

	findCliLocResp, err := coorCli.FindClientLocation(coormq.NewFindClientLocation(stgglb.Local.ExternalIP))
	if err != nil {
		return nil, fmt.Errorf("finding client location: %w", err)
	}

	nodeInfos := lo.Map(getUserNodesResp.Nodes, func(node model.Node, index int) UploadNodeInfo {
		return UploadNodeInfo{
			Node:           node,
			IsSameLocation: node.LocationID == findCliLocResp.Location.LocationID,
		}
	})

	var ecInfo cdssdk.ECRedundancyInfo
	if ecInfo, err = getPkgResp.Package.Redundancy.ToECInfo(); err != nil {
		return nil, fmt.Errorf("get ec redundancy info: %w", err)
	}

	getECResp, err := coorCli.GetECConfig(coormq.NewGetECConfig(ecInfo.ECName))
	if err != nil {
		return nil, fmt.Errorf("getting ec: %w", err)
	}

	// 给上传节点的IPFS加锁
	ipfsReqBlder := reqbuilder.NewBuilder()
	// 如果本地的IPFS也是存储系统的一个节点，那么从本地上传时，需要加锁
	if stgglb.Local.NodeID != nil {
		ipfsReqBlder.IPFS().CreateAnyRep(*stgglb.Local.NodeID)
	}
	for _, node := range nodeInfos {
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

	rets, err := uploadAndUpdateECPackage(t.packageID, t.objectIter, nodeInfos, ecInfo, getECResp.Config)
	if err != nil {
		return nil, err
	}

	return &UpdateECPackageResult{
		ObjectResults: rets,
	}, nil
}
