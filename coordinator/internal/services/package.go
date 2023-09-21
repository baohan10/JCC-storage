package services

import (
	"database/sql"
	"fmt"
	"sort"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	stgsdk "gitlink.org.cn/cloudream/common/sdks/storage"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	scmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

func (svc *Service) GetPackage(msg *coormq.GetPackage) (*coormq.GetPackageResp, *mq.CodeMessage) {
	pkg, err := svc.db.Package().GetByID(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("get package: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get package failed")
	}

	return mq.ReplyOK(coormq.NewGetPackageResp(pkg))
}

func (svc *Service) GetPackageObjects(msg *coormq.GetPackageObjects) (*coormq.GetPackageObjectsResp, *mq.CodeMessage) {
	// TODO 检查用户是否有权限
	objs, err := svc.db.Object().GetPackageObjects(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("get package objects: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get package objects failed")
	}

	return mq.ReplyOK(coormq.NewGetPackageObjectsResp(objs))
}

func (svc *Service) CreatePackage(msg *coormq.CreatePackage) (*coormq.CreatePackageResp, *mq.CodeMessage) {
	var pkgID int64
	err := svc.db.DoTx(sql.LevelDefault, func(tx *sqlx.Tx) error {
		var err error
		pkgID, err = svc.db.Package().Create(svc.db.SQLCtx(), msg.BucketID, msg.Name, msg.Redundancy)
		return err
	})
	if err != nil {
		logger.WithField("BucketID", msg.BucketID).
			WithField("Name", msg.Name).
			Warnf("creating package: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "creating package failed")
	}

	return mq.ReplyOK(coormq.NewCreatePackageResp(pkgID))
}

func (svc *Service) UpdateRepPackage(msg *coormq.UpdateRepPackage) (*coormq.UpdateRepPackageResp, *mq.CodeMessage) {
	_, err := svc.db.Package().GetByID(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("get package: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get package failed")
	}

	err = svc.db.DoTx(sql.LevelDefault, func(tx *sqlx.Tx) error {
		// 先执行删除操作
		if len(msg.Deletes) > 0 {
			if err := svc.db.Object().BatchDelete(tx, msg.Deletes); err != nil {
				return fmt.Errorf("deleting objects: %w", err)
			}
		}

		// 再执行添加操作
		if len(msg.Adds) > 0 {
			if _, err := svc.db.Object().BatchAddRep(tx, msg.PackageID, msg.Adds); err != nil {
				return fmt.Errorf("adding objects: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		logger.Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "update rep package failed")
	}

	// 紧急任务
	var affectFileHashes []string
	for _, add := range msg.Adds {
		affectFileHashes = append(affectFileHashes, add.FileHash)
	}

	err = svc.scanner.PostEvent(scmq.NewPostEvent(scevt.NewCheckRepCount(affectFileHashes), true, true))
	if err != nil {
		logger.Warnf("post event to scanner failed, but this will not affect creating, err: %s", err.Error())
	}

	return mq.ReplyOK(coormq.NewUpdateRepPackageResp())
}

func (svc *Service) UpdateECPackage(msg *coormq.UpdateECPackage) (*coormq.UpdateECPackageResp, *mq.CodeMessage) {
	_, err := svc.db.Package().GetByID(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("get package: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get package failed")
	}

	err = svc.db.DoTx(sql.LevelDefault, func(tx *sqlx.Tx) error {
		// 先执行删除操作
		if len(msg.Deletes) > 0 {
			if err := svc.db.Object().BatchDelete(tx, msg.Deletes); err != nil {
				return fmt.Errorf("deleting objects: %w", err)
			}
		}

		// 再执行添加操作
		if len(msg.Adds) > 0 {
			if _, err := svc.db.Object().BatchAddEC(tx, msg.PackageID, msg.Adds); err != nil {
				return fmt.Errorf("adding objects: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		logger.Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "update ec package failed")
	}

	return mq.ReplyOK(coormq.NewUpdateECPackageResp())
}

func (svc *Service) DeletePackage(msg *coormq.DeletePackage) (*coormq.DeletePackageResp, *mq.CodeMessage) {
	isAva, err := svc.db.Package().IsAvailable(svc.db.SQLCtx(), msg.UserID, msg.PackageID)
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("PackageID", msg.PackageID).
			Warnf("check package available failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "check package available failed")
	}
	if !isAva {
		logger.WithField("UserID", msg.UserID).
			WithField("PackageID", msg.PackageID).
			Warnf("package is not available to the user")
		return nil, mq.Failed(errorcode.OperationFailed, "package is not available to the user")
	}

	err = svc.db.DoTx(sql.LevelDefault, func(tx *sqlx.Tx) error {
		return svc.db.Package().SoftDelete(tx, msg.PackageID)
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("PackageID", msg.PackageID).
			Warnf("set package deleted failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "set package deleted failed")
	}

	stgs, err := svc.db.StoragePackage().FindPackageStorages(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		logger.Warnf("find package storages failed, but this will not affect the deleting, err: %s", err.Error())
		return mq.ReplyOK(coormq.NewDeletePackageResp())
	}

	// 不追求及时、准确
	if len(stgs) == 0 {
		// 如果没有被引用，直接投递CheckPackage的任务
		err := svc.scanner.PostEvent(scmq.NewPostEvent(scevt.NewCheckPackage([]int64{msg.PackageID}), false, false))
		if err != nil {
			logger.Warnf("post event to scanner failed, but this will not affect deleting, err: %s", err.Error())
		}
		logger.Debugf("post check package event")

	} else {
		// 有引用则让Agent去检查StoragePackage
		for _, stg := range stgs {
			err := svc.scanner.PostEvent(scmq.NewPostEvent(scevt.NewAgentCheckStorage(stg.StorageID, []int64{msg.PackageID}), false, false))
			if err != nil {
				logger.Warnf("post event to scanner failed, but this will not affect deleting, err: %s", err.Error())
			}
		}
		logger.Debugf("post agent check storage event")
	}

	return mq.ReplyOK(coormq.NewDeletePackageResp())
}

func (svc *Service) GetPackageCachedNodes(msg *coormq.GetPackageCachedNodes) (*coormq.GetPackageCachedNodesResp, *mq.CodeMessage) {
	isAva, err := svc.db.Package().IsAvailable(svc.db.SQLCtx(), msg.UserID, msg.PackageID)
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("PackageID", msg.PackageID).
			Warnf("check package available failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "check package available failed")
	}
	if !isAva {
		logger.WithField("UserID", msg.UserID).
			WithField("PackageID", msg.PackageID).
			Warnf("package is not available to the user")
		return nil, mq.Failed(errorcode.OperationFailed, "package is not available to the user")
	}

	pkg, err := svc.db.Package().GetByID(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("get package: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get package failed")
	}

	var packageSize int64
	nodeInfoMap := make(map[int64]*stgsdk.NodePackageCachingInfo)
	if pkg.Redundancy.IsRepInfo() {
		// 备份方式为rep
		objectRepDatas, err := svc.db.ObjectRep().GetWithNodeIDInPackage(svc.db.SQLCtx(), msg.PackageID)
		if err != nil {
			logger.WithField("PackageID", msg.PackageID).
				Warnf("get objectRepDatas by packageID failed, err: %s", err.Error())
			return nil, mq.Failed(errorcode.OperationFailed, "get objectRepDatas by packageID failed")
		}

		for _, data := range objectRepDatas {
			packageSize += data.Object.Size
			for _, nodeID := range data.NodeIDs {

				nodeInfo, exists := nodeInfoMap[nodeID]
				if !exists {
					nodeInfo = &stgsdk.NodePackageCachingInfo{
						NodeID:      nodeID,
						FileSize:    data.Object.Size,
						ObjectCount: 1,
					}
				} else {
					nodeInfo.FileSize += data.Object.Size
					nodeInfo.ObjectCount++
				}
				nodeInfoMap[nodeID] = nodeInfo
			}
		}
	} else if pkg.Redundancy.IsECInfo() {
		// 备份方式为ec
		objectECDatas, err := svc.db.ObjectBlock().GetWithNodeIDInPackage(svc.db.SQLCtx(), msg.PackageID)
		if err != nil {
			logger.WithField("PackageID", msg.PackageID).
				Warnf("get objectECDatas by packageID failed, err: %s", err.Error())
			return nil, mq.Failed(errorcode.OperationFailed, "get objectECDatas by packageID failed")
		}

		for _, ecData := range objectECDatas {
			packageSize += ecData.Object.Size
			for _, block := range ecData.Blocks {
				for _, nodeID := range block.NodeIDs {

					nodeInfo, exists := nodeInfoMap[nodeID]
					if !exists {
						nodeInfo = &stgsdk.NodePackageCachingInfo{
							NodeID:      nodeID,
							FileSize:    ecData.Object.Size,
							ObjectCount: 1,
						}
					} else {
						nodeInfo.FileSize += ecData.Object.Size
						nodeInfo.ObjectCount++
					}
					nodeInfoMap[nodeID] = nodeInfo
				}
			}
		}
	} else {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("Redundancy type %s is wrong", pkg.Redundancy.Type)
		return nil, mq.Failed(errorcode.OperationFailed, "redundancy type is wrong")
	}

	var nodeInfos []stgsdk.NodePackageCachingInfo
	for _, nodeInfo := range nodeInfoMap {
		nodeInfos = append(nodeInfos, *nodeInfo)
	}

	sort.Slice(nodeInfos, func(i, j int) bool {
		return nodeInfos[i].NodeID < nodeInfos[j].NodeID
	})
	return mq.ReplyOK(coormq.NewGetPackageCachedNodesResp(nodeInfos, packageSize, pkg.Redundancy.Type))
}

func (svc *Service) GetPackageLoadedNodes(msg *coormq.GetPackageLoadedNodes) (*coormq.GetPackageLoadedNodesResp, *mq.CodeMessage) {
	storages, err := svc.db.StoragePackage().FindPackageStorages(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("get storages by packageID failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get storages by packageID failed")
	}

	uniqueNodeIDs := make(map[int64]bool)
	var nodeIDs []int64
	for _, stg := range storages {
		if !uniqueNodeIDs[stg.NodeID] {
			uniqueNodeIDs[stg.NodeID] = true
			nodeIDs = append(nodeIDs, stg.NodeID)
		}
	}

	return mq.ReplyOK(coormq.NewGetPackageLoadedNodesResp(nodeIDs))
}
