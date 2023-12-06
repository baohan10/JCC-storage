package db

import (
	"time"

	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/consts"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
)

type CacheDB struct {
	*DB
}

func (db *DB) Cache() *CacheDB {
	return &CacheDB{DB: db}
}

func (*CacheDB) Get(ctx SQLContext, fileHash string, nodeID cdssdk.NodeID) (model.Cache, error) {
	var ret model.Cache
	err := sqlx.Get(ctx, &ret, "select * from Cache where FileHash = ? and NodeID = ?", fileHash, nodeID)
	return ret, err
}

func (*CacheDB) BatchGetAllFileHashes(ctx SQLContext, start int, count int) ([]string, error) {
	var ret []string
	err := sqlx.Select(ctx, &ret, "select distinct FileHash from Cache limit ?, ?", start, count)
	return ret, err
}

func (*CacheDB) GetNodeCaches(ctx SQLContext, nodeID cdssdk.NodeID) ([]model.Cache, error) {
	var ret []model.Cache
	err := sqlx.Select(ctx, &ret, "select * from Cache where NodeID = ?", nodeID)
	return ret, err
}

// CreateNew 创建一条新的缓存记录
func (*CacheDB) CreateNew(ctx SQLContext, fileHash string, nodeID cdssdk.NodeID) error {
	_, err := ctx.Exec("insert into Cache values(?,?,?,?,?)", fileHash, nodeID, consts.CacheStatePinned, nil, time.Now())
	if err != nil {
		return err
	}

	return nil
}

func (*CacheDB) SetPackageObjectFrozen(ctx SQLContext, pkgID cdssdk.PackageID, nodeID cdssdk.NodeID) error {
	var nowTime = time.Now()
	_, err := ctx.Exec(
		"insert into Cache(FileHash,NodeID,State,FrozenTime,CreateTime,Priority)"+
			" select FileHash, ?, ?, ?, ?, ? from Object where PackageID = ?"+
			" on duplicate key update State = ?, FrozenTime = ?",
		nodeID, consts.CacheStatePinned, &nowTime, &nowTime, 0,
		pkgID,
		consts.CacheStatePinned, &nowTime,
	)

	return err
}

// CreatePinned 创建一条缓存记录，如果已存在，但不是pinned状态，则将其设置为pin状态
func (*CacheDB) CreatePinned(ctx SQLContext, fileHash string, nodeID cdssdk.NodeID, priority int) error {
	_, err := ctx.Exec("insert into Cache values(?,?,?,?,?,?) on duplicate key update State = ?, CreateTime = ?, Priority = ?",
		fileHash, nodeID, consts.CacheStatePinned, nil, time.Now(), priority,
		consts.CacheStatePinned, time.Now(), priority,
	)
	return err
}

func (*CacheDB) BatchCreatePinned(ctx SQLContext, fileHashes []string, nodeID cdssdk.NodeID, priority int) error {
	var caches []model.Cache
	var nowTime = time.Now()
	for _, hash := range fileHashes {
		caches = append(caches, model.Cache{
			FileHash:   hash,
			NodeID:     nodeID,
			State:      consts.CacheStatePinned,
			FrozenTime: nil,
			CreateTime: nowTime,
			Priority:   priority,
		})
	}

	_, err := sqlx.NamedExec(ctx, "insert into Cache(FileHash,NodeID,State,FrozenTime,CreateTime,Priority) values(:FileHash,:NodeID,:State,:FrozenTime,:CreateTime,:Priority)"+
		" on duplicate key update State=values(State), CreateTime=values(CreateTime), Priority=values(Priority)",
		caches,
	)
	return err
}

// Create 创建一条Temp状态的缓存记录，如果已存在则不产生效果
func (*CacheDB) CreateTemp(ctx SQLContext, fileHash string, nodeID cdssdk.NodeID) error {
	_, err := ctx.Exec("insert ignore into Cache values(?,?,?,?,?)", fileHash, nodeID, nil, consts.CacheStateTemp, time.Now())
	return err
}

// GetCachingFileNodes 查找缓存了指定文件的节点
func (*CacheDB) GetCachingFileNodes(ctx SQLContext, fileHash string) ([]model.Node, error) {
	var x []model.Node
	err := sqlx.Select(ctx, &x,
		"select Node.* from Cache, Node where Cache.FileHash=? and Cache.NodeID = Node.NodeID", fileHash)
	return x, err
}

// DeleteTemp 删除一条Temp状态的记录
func (*CacheDB) DeleteTemp(ctx SQLContext, fileHash string, nodeID cdssdk.NodeID) error {
	_, err := ctx.Exec("delete from Cache where FileHash = ? and NodeID = ? and State = ?", fileHash, nodeID, consts.CacheStateTemp)
	return err
}

// DeleteNodeAll 删除一个节点所有的记录
func (*CacheDB) DeleteNodeAll(ctx SQLContext, nodeID cdssdk.NodeID) error {
	_, err := ctx.Exec("delete from Cache where NodeID = ?", nodeID)
	return err
}

// FindCachingFileUserNodes 在缓存表中查询指定数据所在的节点
func (*CacheDB) FindCachingFileUserNodes(ctx SQLContext, userID cdssdk.NodeID, fileHash string) ([]model.Node, error) {
	var x []model.Node
	err := sqlx.Select(ctx, &x,
		"select Node.* from Cache, UserNode, Node where"+
			" Cache.FileHash=? and Cache.NodeID = UserNode.NodeID and"+
			" UserNode.UserID = ? and UserNode.NodeID = Node.NodeID", fileHash, userID)
	return x, err
}

// 设置一条记录为Temp，对Frozen的记录无效
func (*CacheDB) SetTemp(ctx SQLContext, fileHash string, nodeID cdssdk.NodeID) error {
	_, err := ctx.Exec("update Cache set State = ?, CreateTime = ? where FileHash = ? and NodeID = ? and FrozenTime = null",
		consts.CacheStateTemp,
		time.Now(),
		fileHash,
		nodeID,
	)
	return err
}
