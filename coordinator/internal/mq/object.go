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

	return mq.ReplyOK(coormq.RespPackageObjectDetails(details))
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
	var sucs []cdssdk.ObjectID
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

		avaiUpdatings, notExistsObjs := pickByObjectIDs(msg.Updatings, oldObjIDs, func(obj cdssdk.UpdatingObject) cdssdk.ObjectID { return obj.ObjectID })
		if len(notExistsObjs) > 0 {
			// TODO 部分对象已经不存在
		}

		newObjs := make([]cdssdk.Object, len(avaiUpdatings))
		for i := range newObjs {
			newObjs[i] = oldObjs[i]
			avaiUpdatings[i].ApplyTo(&newObjs[i])
		}

		err = svc.db.Object().BatchUpsertByPackagePath(tx, newObjs)
		if err != nil {
			return fmt.Errorf("batch create or update: %w", err)
		}

		sucs = lo.Map(newObjs, func(obj cdssdk.Object, _ int) cdssdk.ObjectID { return obj.ObjectID })
		return nil
	})

	if err != nil {
		logger.Warnf("batch updating objects: %s", err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "batch update objects failed")
	}

	return mq.ReplyOK(coormq.RespUpdateObjectInfos(sucs))
}

// 根据objIDs从objs中挑选Object。
// len(objs) >= len(objIDs)
func pickByObjectIDs[T any](objs []T, objIDs []cdssdk.ObjectID, getID func(T) cdssdk.ObjectID) (picked []T, notFound []T) {
	objIdx := 0
	idIdx := 0

	for idIdx < len(objIDs) && objIdx < len(objs) {
		if getID(objs[objIdx]) < objIDs[idIdx] {
			notFound = append(notFound, objs[objIdx])
			objIdx++
			continue
		}

		picked = append(picked, objs[objIdx])
		objIdx++
		idIdx++
	}

	return
}

func (svc *Service) MoveObjects(msg *coormq.MoveObjects) (*coormq.MoveObjectsResp, *mq.CodeMessage) {
	var sucs []cdssdk.ObjectID
	err := svc.db.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		msg.Movings = sort2.Sort(msg.Movings, func(o1, o2 cdssdk.MovingObject) int {
			return sort2.Cmp(o1.ObjectID, o2.ObjectID)
		})

		objIDs := make([]cdssdk.ObjectID, len(msg.Movings))
		for i, obj := range msg.Movings {
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

		avaiMovings, notExistsObjs := pickByObjectIDs(msg.Movings, oldObjIDs, func(obj cdssdk.MovingObject) cdssdk.ObjectID { return obj.ObjectID })
		if len(notExistsObjs) > 0 {
			// TODO 部分对象已经不存在
		}

		// 筛选出PackageID变化、Path变化的对象，这两种对象要检测改变后是否有冲突
		var pkgIDChangedObjs []cdssdk.Object
		var pathChangedObjs []cdssdk.Object
		for i := range avaiMovings {
			if avaiMovings[i].PackageID != oldObjs[i].PackageID {
				newObj := oldObjs[i]
				avaiMovings[i].ApplyTo(&newObj)
				pkgIDChangedObjs = append(pkgIDChangedObjs, newObj)
			} else if avaiMovings[i].Path != oldObjs[i].Path {
				newObj := oldObjs[i]
				avaiMovings[i].ApplyTo(&newObj)
				pathChangedObjs = append(pathChangedObjs, newObj)
			}
		}

		var newObjs []cdssdk.Object
		// 对于PackageID发生变化的对象，需要检查目标Package内是否存在同Path的对象
		ensuredObjs, err := svc.ensurePackageChangedObjects(tx, msg.UserID, pkgIDChangedObjs)
		if err != nil {
			return err
		}
		newObjs = append(newObjs, ensuredObjs...)

		// 对于只有Path发生变化的对象，则检查同Package内有没有同Path的对象
		ensuredObjs, err = svc.ensurePathChangedObjects(tx, msg.UserID, pathChangedObjs)
		if err != nil {
			return err
		}
		newObjs = append(newObjs, ensuredObjs...)

		err = svc.db.Object().BatchUpert(tx, newObjs)
		if err != nil {
			return fmt.Errorf("batch create or update: %w", err)
		}

		sucs = lo.Map(newObjs, func(obj cdssdk.Object, _ int) cdssdk.ObjectID { return obj.ObjectID })
		return nil
	})
	if err != nil {
		logger.Warn(err.Error())
		return nil, mq.Failed(errorcode.OperationFailed, "move objects failed")
	}

	return mq.ReplyOK(coormq.RespMoveObjects(sucs))
}

func (svc *Service) ensurePackageChangedObjects(tx *sqlx.Tx, userID cdssdk.UserID, objs []cdssdk.Object) ([]cdssdk.Object, error) {
	if len(objs) == 0 {
		return nil, nil
	}

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
			// TODO 有两个对象移动到同一个路径，有冲突
		}
	}

	var willUpdateObjs []cdssdk.Object
	for _, pkg := range packages {
		_, err := svc.db.Package().GetUserPackage(tx, userID, pkg.PackageID)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("getting user package by id: %w", err)
		}

		existsObjs, err := svc.db.Object().BatchGetByPackagePath(tx, pkg.PackageID, lo.Keys(pkg.ObjectByPath))
		if err != nil {
			return nil, fmt.Errorf("batch getting objects by package path: %w", err)
		}

		// 标记冲突的对象
		for _, obj := range existsObjs {
			pkg.ObjectByPath[obj.Path] = nil
			// TODO 目标Package内有冲突的对象
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

func (svc *Service) ensurePathChangedObjects(tx *sqlx.Tx, userID cdssdk.UserID, objs []cdssdk.Object) ([]cdssdk.Object, error) {
	if len(objs) == 0 {
		return nil, nil
	}

	objByPath := make(map[string]*cdssdk.Object)
	for _, obj := range objs {
		if objByPath[obj.Path] == nil {
			o := obj
			objByPath[obj.Path] = &o
		} else {
			// TODO 有两个对象移动到同一个路径，有冲突
		}

	}

	_, err := svc.db.Package().GetUserPackage(tx, userID, objs[0].PackageID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting user package by id: %w", err)
	}

	existsObjs, err := svc.db.Object().BatchGetByPackagePath(tx, objs[0].PackageID, lo.Map(objs, func(obj cdssdk.Object, idx int) string { return obj.Path }))
	if err != nil {
		return nil, fmt.Errorf("batch getting objects by package path: %w", err)
	}

	// 不支持两个对象交换位置的情况，因为数据库不支持
	for _, obj := range existsObjs {
		objByPath[obj.Path] = nil
	}

	var willMoveObjs []cdssdk.Object
	for _, obj := range objByPath {
		if obj == nil {
			continue
		}
		willMoveObjs = append(willMoveObjs, *obj)
	}

	return willMoveObjs, nil
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
