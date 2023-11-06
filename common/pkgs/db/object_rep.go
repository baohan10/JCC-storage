package db

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type ObjectRepDB struct {
	*DB
}

func (db *DB) ObjectRep() *ObjectRepDB {
	return &ObjectRepDB{DB: db}
}

// GetObjectRep 查询对象副本表
func (db *ObjectRepDB) GetByID(ctx SQLContext, objectID int64) (model.ObjectRep, error) {
	var ret model.ObjectRep
	err := sqlx.Get(ctx, &ret, "select * from ObjectRep where ObjectID = ?", objectID)
	return ret, err
}

func (db *ObjectRepDB) Create(ctx SQLContext, objectID int64, fileHash string) error {
	_, err := ctx.Exec("insert into ObjectRep(ObjectID, FileHash) values(?,?)", objectID, fileHash)
	return err
}

func (db *ObjectRepDB) Update(ctx SQLContext, objectID int64, fileHash string) (int64, error) {
	ret, err := ctx.Exec("update ObjectRep set FileHash = ? where ObjectID = ?", fileHash, objectID)
	if err != nil {
		return 0, err
	}

	cnt, err := ret.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get affected rows failed, err: %w", err)
	}

	return cnt, nil
}

func (db *ObjectRepDB) Delete(ctx SQLContext, objectID int64) error {
	_, err := ctx.Exec("delete from ObjectRep where ObjectID = ?", objectID)
	return err
}

func (db *ObjectRepDB) DeleteInPackage(ctx SQLContext, packageID int64) error {
	_, err := ctx.Exec("delete ObjectRep from ObjectRep inner join Object on ObjectRep.ObjectID = Object.ObjectID where PackageID = ?", packageID)
	return err
}

func (db *ObjectRepDB) GetFileMaxRepCount(ctx SQLContext, fileHash string) (int, error) {
	var maxRepCnt *int
	err := sqlx.Get(ctx, &maxRepCnt,
		"select json_extract(Redundancy, '$.info.repCount') from ObjectRep, Object, Package where FileHash = ? and"+
			" ObjectRep.ObjectID = Object.ObjectID and"+
			" Object.PackageID = Package.PackageID and"+
			" Package.State = ?", fileHash, consts.PackageStateNormal)

	if err == sql.ErrNoRows {
		return 0, nil
	}

	if err != nil {
		return 0, err
	}

	if maxRepCnt == nil {
		return 0, nil
	}

	return *maxRepCnt, err
}

func (db *ObjectRepDB) GetWithNodeIDInPackage(ctx SQLContext, packageID int64) ([]stgmod.ObjectRepData, error) {
	var tmpRets []struct {
		model.Object
		FileHash *string `db:"FileHash"`
		NodeIDs  *string `db:"NodeIDs"`
	}

	err := sqlx.Select(ctx,
		&tmpRets,
		"select Object.*, ObjectRep.FileHash, group_concat(NodeID) as NodeIDs from Object"+
			" left join ObjectRep on Object.ObjectID = ObjectRep.ObjectID"+
			" left join Cache on ObjectRep.FileHash = Cache.FileHash"+
			" where PackageID = ? group by Object.ObjectID order by Object.ObjectID asc",
		packageID,
	)
	if err != nil {
		return nil, err
	}
	rets := make([]stgmod.ObjectRepData, 0, len(tmpRets))
	for _, tmp := range tmpRets {
		var repData stgmod.ObjectRepData
		repData.Object = tmp.Object

		if tmp.FileHash != nil {
			repData.FileHash = *tmp.FileHash
		}

		if tmp.NodeIDs != nil {
			repData.NodeIDs = splitIDStringUnsafe(*tmp.NodeIDs)
		}

		rets = append(rets, repData)
	}

	return rets, nil
}

func (db *ObjectRepDB) GetPackageObjectCacheInfos(ctx SQLContext, packageID int64) ([]cdssdk.ObjectCacheInfo, error) {
	var tmpRet []struct {
		cdssdk.Object
		FileHash string `db:"FileHash"`
	}

	err := sqlx.Select(ctx, &tmpRet, "select Object.*, ObjectRep.FileHash from Object"+
		" left join ObjectRep on Object.ObjectID = ObjectRep.ObjectID"+
		" where Object.PackageID = ? order by Object.ObjectID asc", packageID)
	if err != nil {
		return nil, err
	}

	ret := make([]cdssdk.ObjectCacheInfo, len(tmpRet))
	for i, r := range tmpRet {
		ret[i] = cdssdk.NewObjectCacheInfo(r.Object, r.FileHash)
	}

	return ret, nil
}

// 按逗号切割字符串，并将每一个部分解析为一个int64的ID。
// 注：需要外部保证分隔的每一个部分都是正确的10进制数字格式
func splitIDStringUnsafe(idStr string) []int64 {
	idStrs := strings.Split(idStr, ",")
	ids := make([]int64, 0, len(idStrs))

	for _, str := range idStrs {
		// 假设传入的ID是正确的数字格式
		id, _ := strconv.ParseInt(str, 10, 64)
		ids = append(ids, id)
	}

	return ids
}
