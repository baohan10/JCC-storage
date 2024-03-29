package mq

import (
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts/errorcode"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/mq"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

func (svc *Service) GetPackageObjects(msg *coormq.GetPackageObjects) (*coormq.GetPackageObjectsResp, *mq.CodeMessage) {
	var objs []cdssdk.Object
	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		_, err := svc.db.Package().GetUserPackage(tx, msg.UserID, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package by id: %w", err)
		}

		objs, err = svc.db.Object().GetPackageObjects(svc.db.SQLCtx(), msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package objects: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.WithField("UserID", msg.UserID).WithField("PackageID", msg.PackageID).
			Warn(err.Error())

		return nil, mq.Failed(errorcode.OperationFailed, "get package objects failed")
	}

	return mq.ReplyOK(coormq.NewGetPackageObjectsResp(objs))
}

func (svc *Service) GetPackageObjectDetails(msg *coormq.GetPackageObjectDetails) (*coormq.GetPackageObjectDetailsResp, *mq.CodeMessage) {
	var details []stgmod.ObjectDetail
	// 必须放在事务里进行，因为GetPackageBlockDetails是由多次数据库操作组成，必须保证数据的一致性
	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		var err error
		_, err = svc.db.Package().GetByID(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package by id: %w", err)
		}

		details, err = svc.db.Object().GetPackageObjectDetails(tx, msg.PackageID)
		if err != nil {
			return fmt.Errorf("getting package block details: %w", err)
		}

		return nil
	})

	if err != nil {
		logger.WithField("PackageID", msg.PackageID).Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get package object block details failed")
	}

	return mq.ReplyOK(coormq.NewGetPackageObjectDetailsResp(details))
}

func (svc *Service) GetObjectDetails(msg *coormq.GetObjectDetails) (*coormq.GetObjectDetailsResp, *mq.CodeMessage) {
	details := make([]*stgmod.ObjectDetail, len(msg.ObjectIDs))
	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		var err error

		msg.ObjectIDs = sort2.SortAsc(msg.ObjectIDs)

		// 根据ID依次查询Object，ObjectBlock，PinnedObject，并根据升序的特点进行合并
		objs, err := svc.db.Object().BatchGet(tx, msg.ObjectIDs)
		if err != nil {
			return fmt.Errorf("batch get objects: %w", err)
		}

		objIDIdx := 0
		objIdx := 0
		for objIDIdx < len(msg.ObjectIDs) && objIdx < len(objs) {
			if msg.ObjectIDs[objIDIdx] < objs[objIdx].ObjectID {
				objIDIdx++
				continue
			}

			// 由于是使用msg.ObjectIDs去查询Object，因此不存在msg.ObjectIDs > Object.ObjectID的情况，
			// 下面同理
			obj := stgmod.ObjectDetail{
				Object: objs[objIDIdx],
			}
			details[objIDIdx] = &obj
			objIdx++
		}

		// 查询合并
		blocks, err := svc.db.ObjectBlock().BatchGetByObjectID(tx, msg.ObjectIDs)
		if err != nil {
			return fmt.Errorf("batch get object blocks: %w", err)
		}

		objIDIdx = 0
		blkIdx := 0
		for objIDIdx < len(msg.ObjectIDs) && blkIdx < len(blocks) {
			if details[objIDIdx] == nil {
				objIDIdx++
				continue
			}

			if msg.ObjectIDs[objIDIdx] < blocks[blkIdx].ObjectID {
				objIDIdx++
				continue
			}

			details[objIDIdx].Blocks = append(details[objIDIdx].Blocks, blocks[blkIdx])
			blkIdx++
		}

		// 查询合并
		pinneds, err := svc.db.PinnedObject().BatchGetByObjectID(tx, msg.ObjectIDs)
		if err != nil {
			return fmt.Errorf("batch get pinned objects: %w", err)
		}

		objIDIdx = 0
		pinIdx := 0
		for objIDIdx < len(msg.ObjectIDs) && pinIdx < len(pinneds) {
			if details[objIDIdx] == nil {
				objIDIdx++
				continue
			}

			if msg.ObjectIDs[objIDIdx] < pinneds[pinIdx].ObjectID {
				objIDIdx++
				continue
			}

			details[objIDIdx].PinnedAt = append(details[objIDIdx].PinnedAt, pinneds[pinIdx].NodeID)
			pinIdx++
		}
		return nil
	})

	if err != nil {
		logger.Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "get object details failed")
	}

	return mq.ReplyOK(coormq.RespGetObjectDetails(details))
}

func (svc *Service) UpdateObjectRedundancy(msg *coormq.UpdateObjectRedundancy) (*coormq.UpdateObjectRedundancyResp, *mq.CodeMessage) {
	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		return svc.db.Object().BatchUpdateRedundancy(tx, msg.Updatings)
	})
	if err != nil {
		logger.Warnf("batch updating redundancy: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "batch update redundancy failed")
	}

	return mq.ReplyOK(coormq.RespUpdateObjectRedundancy())
}

func (svc *Service) UpdateObjectInfos(msg *coormq.UpdateObjectInfos) (*coormq.UpdateObjectInfosResp, *mq.CodeMessage) {
	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		msg.Updatings = sort2.Sort(msg.Updatings, func(o1, o2 cdssdk.UpdatingObject) int {
			return sort2.Cmp(o1.ObjectID, o2.ObjectID)
		})

		objIDs := make([]cdssdk.ObjectID, len(msg.Updatings))
		for i, obj := range msg.Updatings {
			objIDs[i] = obj.ObjectID
		}

		oldObjs, err := svc.db.Object().BatchGet(tx, objIDs)
		if err != nil {
			return fmt.Errorf("batch getting objects: %w", err)
		}
		oldObjIDs := make([]cdssdk.ObjectID, len(oldObjs))
		for i, obj := range oldObjs {
			oldObjIDs[i] = obj.ObjectID
		}

		avaiUpdatings, notExistsObjs := pickByObjectIDs(msg.Updatings, oldObjIDs)
		if len(notExistsObjs) > 0 {
			// TODO 部分对象已经不存在
		}

		// 筛选出PackageID变化、Path变化的对象，这两种对象要检测改变后是否有冲突
		// 否则，直接更新即可
		//var pkgIDChangedObjs []cdssdk.Object
		//var pathChangedObjs []cdssdk.Object
		//var infoChangedObjs []cdssdk.Object
		//for i := range willUpdateObjs {
		//	if willUpdateObjs[i].PackageID != oldObjs[i].PackageID {
		//		newObj := oldObjs[i]
		//		willUpdateObjs[i].ApplyTo(&newObj)
		//		pkgIDChangedObjs = append(pkgIDChangedObjs, newObj)
		//	} else if willUpdateObjs[i].Path != oldObjs[i].Path {
		//		newObj := oldObjs[i]
		//		willUpdateObjs[i].ApplyTo(&newObj)
		//		pathChangedObjs = append(pathChangedObjs, newObj)
		//	} else {
		//		newObj := oldObjs[i]
		//		willUpdateObjs[i].ApplyTo(&newObj)
		//		infoChangedObjs = append(infoChangedObjs, newObj)
		//	}
		//}

		newObjs := make([]cdssdk.Object, len(avaiUpdatings))
		for i := range newObjs {
			newObjs[i] = oldObjs[i]
			avaiUpdatings[i].ApplyTo(&newObjs[i])
		}

		err = svc.db.Object().BatchCreateOrUpdate(tx, newObjs)
		if err != nil {
			return fmt.Errorf("batch create or update: %w", err)
		}

		return nil
	})

	if err != nil {
		logger.Warnf("batch updating objects: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "batch update objects failed")
	}

	return mq.ReplyOK(coormq.RespUpdateObjectInfos())
}

// 根据objIDs从objs中挑选Object。
// len(objs) >= len(objIDs)
func pickByObjectIDs(objs []cdssdk.UpdatingObject, objIDs []cdssdk.ObjectID) (pickedObjs []cdssdk.UpdatingObject, notFoundObjs []cdssdk.UpdatingObject) {
	objIdx := 0
	idIdx := 0

	for idIdx < len(objIDs) && objIdx < len(objs) {
		if objs[objIdx].ObjectID < objIDs[idIdx] {
			notFoundObjs = append(notFoundObjs, objs[objIdx])
			objIdx++
			continue
		}

		pickedObjs = append(pickedObjs, objs[objIdx])
		objIdx++
		idIdx++
	}

	return
}

func (svc *Service) ensurePackageChangedObjects(tx *sqlx.Tx, objs []cdssdk.Object) ([]cdssdk.Object, error) {
	type PackageObjects struct {
		PackageID    cdssdk.PackageID
		ObjectByPath map[string]*cdssdk.Object
	}

	packages := make(map[cdssdk.PackageID]*PackageObjects)
	for _, obj := range objs {
		pkg, ok := packages[obj.PackageID]
		if !ok {
			pkg = &PackageObjects{
				PackageID:    obj.PackageID,
				ObjectByPath: make(map[string]*cdssdk.Object),
			}
			packages[obj.PackageID] = pkg
		}

		if pkg.ObjectByPath[obj.Path] == nil {
			o := obj
			pkg.ObjectByPath[obj.Path] = &o
		} else {
			// TODO 有冲突
		}
	}

	var willUpdateObjs []cdssdk.Object
	for _, pkg := range packages {
		existsObjs, err := svc.db.Object().BatchByPackagePath(tx, pkg.PackageID, lo.Keys(pkg.ObjectByPath))
		if err != nil {
			return nil, fmt.Errorf("batch getting objects by package path: %w", err)
		}

		for _, obj := range existsObjs {
			pkg.ObjectByPath[obj.Path] = nil
		}

		for _, obj := range pkg.ObjectByPath {
			if obj == nil {
				continue
			}
			willUpdateObjs = append(willUpdateObjs, *obj)
		}

	}

	return willUpdateObjs, nil
}

func (svc *Service) DeleteObjects(msg *coormq.DeleteObjects) (*coormq.DeleteObjectsResp, *mq.CodeMessage) {
	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		err := svc.db.Object().BatchDelete(tx, msg.ObjectIDs)
		if err != nil {
			return fmt.Errorf("batch deleting objects: %w", err)
		}

		err = svc.db.ObjectBlock().BatchDeleteByObjectID(tx, msg.ObjectIDs)
		if err != nil {
			return fmt.Errorf("batch deleting object blocks: %w", err)
		}

		err = svc.db.PinnedObject().BatchDeleteByObjectID(tx, msg.ObjectIDs)
		if err != nil {
			return fmt.Errorf("batch deleting pinned objects: %w", err)
		}

		return nil
	})
	if err != nil {
		logger.Warnf("batch deleting objects: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "batch delete objects failed")
	}

	return mq.ReplyOK(coormq.RespDeleteObjects())
}
