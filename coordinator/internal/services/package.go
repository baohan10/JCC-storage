package services

import (
	"database/sql"
	"fmt"
	"sort"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
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

func (svc *Service) CreatePackage(msg *coormq.CreatePackage) (*coormq.CreatePackageResp, *mq.CodeMessage) {
	var pkgID cdssdk.PackageID
	err := svc.db.DoTx(sql.LevelLinearizable, func(tx *sqlx.Tx) error {
		var err error

		isAvai, _ := svc.db.Bucket().IsAvailable(tx, msg.BucketID, msg.UserID)
		if !isAvai {
			return fmt.Errorf("bucket is not avaiable to the user")
		}

		pkgID, err = svc.db.Package().Create(tx, msg.BucketID, msg.Name)
		if err != nil {
			return fmt.Errorf("creating package: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.WithField("BucketID", msg.BucketID).
			WithField("Name", msg.Name).
			Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "creating package failed")
	}

	return mq.ReplyOK(coormq.NewCreatePackageResp(pkgID))
}

func (svc *Service) UpdatePackage(msg *coormq.UpdatePackage) (*coormq.UpdatePackageResp, *mq.CodeMessage) {
	err := svc.db.DoTx(sql.LevelLinearizable, func(tx *sqlx.Tx) error {
		_, err := svc.db.Package().GetByID(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package by id: %w", err)
		}

		// 先执行删除操作
		if len(msg.Deletes) > 0 {
			if err := svc.db.Object().BatchDelete(tx, msg.Deletes); err != nil {
				return fmt.Errorf("deleting objects: %w", err)
			}
		}

		// 再执行添加操作
		if len(msg.Adds) > 0 {
			if _, err := svc.db.Object().BatchAdd(tx, msg.PackageID, msg.Adds); err != nil {
				return fmt.Errorf("adding objects: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "update package failed")
	}

	return mq.ReplyOK(coormq.NewUpdatePackageResp())
}

func (svc *Service) DeletePackage(msg *coormq.DeletePackage) (*coormq.DeletePackageResp, *mq.CodeMessage) {
	err := svc.db.DoTx(sql.LevelLinearizable, func(tx *sqlx.Tx) error {
		isAvai, _ := svc.db.Package().IsAvailable(tx, msg.UserID, msg.PackageID)
		if !isAvai {
			return fmt.Errorf("package is not available to the user")
		}

		err := svc.db.Package().SoftDelete(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("soft delete package: %w", err)
		}

		err = svc.db.Package().DeleteUnused(tx, msg.PackageID)
		if err != nil {
			logger.WithField("UserID", msg.UserID).
				WithField("PackageID", msg.PackageID).
				Warnf("deleting unused package: %w", err.Error())
		}

		return nil
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).
			WithField("PackageID", msg.PackageID).
			Warnf(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "delete package failed")
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

	// 这个函数只是统计哪些节点缓存了Package中的数据，不需要多么精确，所以可以不用事务
	objDetails, err := svc.db.ObjectBlock().GetPackageBlockDetails(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("get package block details: %s", err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get package block details failed")
	}

	var packageSize int64
	nodeInfoMap := make(map[cdssdk.NodeID]*cdssdk.NodePackageCachingInfo)
	for _, obj := range objDetails {
		// 只要存了文件的一个块，就认为此节点存了整个文件
		for _, block := range obj.Blocks {
			for _, nodeID := range block.CachedNodeIDs {
				info, ok := nodeInfoMap[nodeID]
				if !ok {
					info = &cdssdk.NodePackageCachingInfo{
						NodeID: nodeID,
					}
					nodeInfoMap[nodeID] = info

				}

				info.FileSize += obj.Object.Size
				info.ObjectCount++
			}
		}
	}

	var nodeInfos []cdssdk.NodePackageCachingInfo
	for _, nodeInfo := range nodeInfoMap {
		nodeInfos = append(nodeInfos, *nodeInfo)
	}

	sort.Slice(nodeInfos, func(i, j int) bool {
		return nodeInfos[i].NodeID < nodeInfos[j].NodeID
	})
	return mq.ReplyOK(coormq.NewGetPackageCachedNodesResp(nodeInfos, packageSize))
}

func (svc *Service) GetPackageLoadedNodes(msg *coormq.GetPackageLoadedNodes) (*coormq.GetPackageLoadedNodesResp, *mq.CodeMessage) {
	storages, err := svc.db.StoragePackage().FindPackageStorages(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("get storages by packageID failed, err: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get storages by packageID failed")
	}

	uniqueNodeIDs := make(map[cdssdk.NodeID]bool)
	var nodeIDs []cdssdk.NodeID
	for _, stg := range storages {
		if !uniqueNodeIDs[stg.NodeID] {
			uniqueNodeIDs[stg.NodeID] = true
			nodeIDs = append(nodeIDs, stg.NodeID)
		}
	}

	return mq.ReplyOK(coormq.NewGetPackageLoadedNodesResp(nodeIDs))
}

func (svc *Service) GetPackageLoadLogDetails(msg *coormq.GetPackageLoadLogDetails) (*coormq.GetPackageLoadLogDetailsResp, *mq.CodeMessage) {
	var logs []coormq.PackageLoadLogDetail
	rawLogs, err := svc.db.StoragePackageLog().GetByPackageID(svc.db.SQLCtx(), msg.PackageID)
	if err != nil {
		logger.WithField("PackageID", msg.PackageID).
			Warnf("getting storage package log: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get storage package log failed")
	}

	stgs := make(map[cdssdk.StorageID]model.Storage)

	for _, raw := range rawLogs {
		stg, ok := stgs[raw.StorageID]
		if !ok {
			stg, err = svc.db.Storage().GetByID(svc.db.SQLCtx(), raw.StorageID)
			if err != nil {
				logger.WithField("PackageID", msg.PackageID).
					Warnf("getting storage: %s", err.Error())
				return nil, mq.Failed(errorcode.OperationFailed, "get storage failed")
			}

			stgs[raw.StorageID] = stg
		}

		logs = append(logs, coormq.PackageLoadLogDetail{
			Storage:    stg,
			UserID:     raw.UserID,
			CreateTime: raw.CreateTime,
		})
	}

	return mq.ReplyOK(coormq.RespGetPackageLoadLogDetails(logs))
}
