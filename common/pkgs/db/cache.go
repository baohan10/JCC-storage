package db

import (
	"time"

	"github.com/jmoiron/sqlx"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
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

func (*CacheDB) GetByNodeID(ctx SQLContext, nodeID cdssdk.NodeID) ([]model.Cache, error) {
	var ret []model.Cache
	err := sqlx.Select(ctx, &ret, "select * from Cache where NodeID = ?", nodeID)
	return ret, err
}

// Create 创建一条的缓存记录，如果已有则不进行操作
func (*CacheDB) Create(ctx SQLContext, fileHash string, nodeID cdssdk.NodeID, priority int) error {
	_, err := ctx.Exec("insert ignore into Cache values(?,?,?,?)", fileHash, nodeID, time.Now(), priority)
	if err != nil {
		return err
	}

	return nil
}

// 批量创建缓存记录
func (*CacheDB) BatchCreate(ctx SQLContext, caches []model.Cache) error {
	return BatchNamedExec(
		ctx,
		"insert into Cache(FileHash,NodeID,CreateTime,Priority) values(:FileHash,:NodeID,:CreateTime,:Priority)"+
			" on duplicate key update CreateTime=values(CreateTime), Priority=values(Priority)",
		4,
		caches,
		nil,
	)
}

func (*CacheDB) BatchCreateOnSameNode(ctx SQLContext, fileHashes []string, nodeID cdssdk.NodeID, priority int) error {
	var caches []model.Cache
	var nowTime = time.Now()
	for _, hash := range fileHashes {
		caches = append(caches, model.Cache{
			FileHash:   hash,
			NodeID:     nodeID,
			CreateTime: nowTime,
			Priority:   priority,
		})
	}

	_, err := sqlx.NamedExec(ctx,
		"insert into Cache(FileHash,NodeID,CreateTime,Priority) values(:FileHash,:NodeID,:CreateTime,:Priority)"+
			" on duplicate key update CreateTime=values(CreateTime), Priority=values(Priority)",
		caches,
	)
	return err
}

func (*CacheDB) NodeBatchDelete(ctx SQLContext, nodeID cdssdk.NodeID, fileHashes []string) error {
	// TODO in语句有长度限制
	query, args, err := sqlx.In("delete from Cache where NodeID = ? and FileHash in (?)", nodeID, fileHashes)
	if err != nil {
		return err
	}
	_, err = ctx.Exec(query, args...)
	return err
}

// GetCachingFileNodes 查找缓存了指定文件的节点
func (*CacheDB) GetCachingFileNodes(ctx SQLContext, fileHash string) ([]cdssdk.Node, error) {
	var x []cdssdk.Node
	err := sqlx.Select(ctx, &x,
		"select Node.* from Cache, Node where Cache.FileHash=? and Cache.NodeID = Node.NodeID", fileHash)
	return x, err
}

// DeleteNodeAll 删除一个节点所有的记录
func (*CacheDB) DeleteNodeAll(ctx SQLContext, nodeID cdssdk.NodeID) error {
	_, err := ctx.Exec("delete from Cache where NodeID = ?", nodeID)
	return err
}

// FindCachingFileUserNodes 在缓存表中查询指定数据所在的节点
func (*CacheDB) FindCachingFileUserNodes(ctx SQLContext, userID cdssdk.NodeID, fileHash string) ([]cdssdk.Node, error) {
	var x []cdssdk.Node
	err := sqlx.Select(ctx, &x,
		"select Node.* from Cache, UserNode, Node where"+
			" Cache.FileHash=? and Cache.NodeID = UserNode.NodeID and"+
			" UserNode.UserID = ? and UserNode.NodeID = Node.NodeID", fileHash, userID)
	return x, err
}
