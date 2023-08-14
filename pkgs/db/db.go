package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/config"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
)

type DB struct {
	d *sqlx.DB
}

type SQLContext interface {
	sqlx.Queryer
	sqlx.Execer
}

func NewDB(cfg *config.Config) (*DB, error) {
	db, err := sqlx.Open("mysql", cfg.MakeSourceString())
	if err != nil {
		return nil, fmt.Errorf("open database connection failed, err: %w", err)
	}

	// 尝试连接一下数据库，如果数据库配置有错误在这里就能报出来
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return &DB{
		d: db,
	}, nil
}

func (db *DB) DoTx(isolation sql.IsolationLevel, fn func(tx *sqlx.Tx) error) error {
	tx, err := db.d.BeginTxx(context.Background(), &sql.TxOptions{Isolation: isolation})
	if err != nil {
		return err
	}

	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return err
	}

	return nil
}

func (db *DB) SQLCtx() SQLContext {
	return db.d
}

// 纠删码对象表插入
// TODO 需要使用事务保证查询之后插入的正确性
func (db *DB) InsertECObject(objectName string, bucketID int, fileSize int64, ecName string) (int64, error) {
	// TODO 参考CreateRepObject重写
	// 根据objectname和bucketid查询，若不存在则插入，若存在则不操作
	//查询
	/*type Object struct {
		ObjectID int64  `db:"ObjectID"`
		Name     string `db:"Name"`
		BucketID int    `db:"BucketID"`
	}
	var x Object
	err := db.d.Get(&x, "select ObjectID, Name, BucketID from Object where Name=? AND BucketID=?", objectName, bucketID)
	//不存在才插入
	if errors.Is(err, sql.ErrNoRows) {
		sql := "insert into Object(Name, BucketID, FileSize, Redundancy, NumRep, EcName) values(?,?,?,?,?,?)"
		r, err := db.d.Exec(sql, objectName, bucketID, fileSize, false, "-1", ecName)
		if err != nil {
			return 0, err
		}

		id, err := r.LastInsertId()
		if err != nil {
			return 0, err
		}

		// TODO 需要考虑失败后的处理

		return id, nil

	} else if err == nil {
		return x.ObjectID, nil
	}
	return 0, err*/
	panic("not implement yet")
}

// QueryObjectBlock 查询对象编码块表
func (db *DB) QueryObjectBlock(objectID int64) ([]model.ObjectBlock, error) {
	var x []model.ObjectBlock
	sql := "select * from ObjectBlock where ObjectID=?"
	err := db.d.Select(&x, sql, objectID)
	return x, err
}

// 对象编码块表Echash插入
func (db *DB) InsertECHash(objectID int64, hashes []string) {
	for i := 0; i < len(hashes); i++ {
		sql := "update ObjectBlock set BlockHash =? where ObjectID = ? AND InnerID = ?"
		// TODO 需要处理错误
		db.d.Exec(sql, hashes[i], objectID, i)
	}
}

// 对象编码块表插入
func (db *DB) InsertEcObjectBlock(objectID int64, innerID int) error {
	// 根据objectID查询，若不存在则插入，若存在则不操作
	_, err := db.d.Exec(
		"insert into ObjectBlock(ObjectID, InnerID) select ?, ? where not exists (select ObjectID from ObjectBlock where ObjectID=? AND InnerID=?)",
		objectID,
		innerID,
		objectID,
		innerID,
	)
	return err
}

// BatchInsertOrUpdateCache 批量更新缓存表
func (db *DB) BatchInsertOrUpdateCache(blockHashes []string, nodeID int64) error {
	//jh:将hashs中的hash，IP插入缓存表中，TempOrPin字段为true，Time为插入时的时间戳
	//-如果要插入的hash、IP在表中已存在且所对应的TempOrPin字段为false，则不做任何操作
	//-如果要插入的hash、IP在表中已存在且所对应的TempOrPin字段为true，则更新Time
	tx, err := db.d.BeginTxx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return fmt.Errorf("start transaction failed, err: %w", err)
	}

	for _, blockHash := range blockHashes {
		//根据hash和nodeip查询缓存表里是否存在此条记录
		var cache model.Cache
		err := tx.Get(
			&cache,
			"select NodeID, TempOrPin, Cachetime from Cache where FileHash=? AND NodeID=?",
			blockHash,
			nodeID,
		)

		// 不存在记录则创建新记录
		if errors.Is(err, sql.ErrNoRows) {
			_, err := tx.Exec("insert into Cache values(?,?,?,?)", blockHash, nodeID, true, time.Now())
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("insert cache failed, err: %w", err)
			}

		} else if err == nil && cache.State == consts.CacheStateTemp {
			//若在表中已存在且所对应的TempOrPin字段为true，则更新Time
			_, err := tx.Exec(
				"update Cache set Cachetime=? where FileHash=? AND NodeID=?",
				time.Now(),
				blockHash,
				nodeID,
			)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("update cache failed, err: %w", err)
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("commit transaction failed, err: %w", err)
	}

	return nil
}

// 查询节点延迟表
func (db *DB) QueryNodeDelay(inNodeIP string, outNodeIP string) (int, error) {
	//节点延迟结构体
	var x struct {
		DelayInMs int `db:"DelayInMs"`
	}
	sql := "select DelayInMs from NodeDelay where InNodeIP=? AND OutNodeIP=?"
	err := db.d.Get(&x, sql, inNodeIP, outNodeIP)
	return x.DelayInMs, err
}

// 节点延迟表插入
// TODO 需要使用事务确保插入的记录完整
func (db *DB) InsertNodeDelay(srcNodeID int64, dstNodeIDs []int64, delay []int) {
	insSql := "insert into NodeDelay values(?,?,?)"
	updateSql := "UPDATE NodeDelay SET DelayInMs=? WHERE SourceNodeID=? AND DestinationNodeID=?"
	for i := 0; i < len(dstNodeIDs); i++ {
		_, err := db.d.Exec(insSql, srcNodeID, dstNodeIDs[i], delay[i])
		if err != nil {
			// TODO 处理错误
			db.d.Exec(updateSql, delay[i], srcNodeID, dstNodeIDs[i])
		}
	}
}

// 节点表插入
// TODO 需要使用事务保证查询之后插入的正确性
func (db *DB) InsertNode(nodeip string, nodelocation string, ipfsstatus string, localdirstatus string) error {
	// 根据NodeIP查询，若不存在则插入，若存在则更新
	//查询
	type Node struct {
		NodeIP string `db:"NodeIP"`
	}
	var x Node
	err := db.d.Get(&x, "select NodeIP from Node where NodeIP=?", nodeip)
	//local和ipfs同时可达才可达
	// TODO 将status字段改成字符串（枚举值）
	NodeStatus := ipfsstatus == consts.IPFSStateOK && localdirstatus == consts.StorageDirectoryStateOK
	//不存在才插入
	if errors.Is(err, sql.ErrNoRows) {
		sql := "insert into Node values(?,?,?)"
		_, err := db.d.Exec(sql, nodeip, nodelocation, NodeStatus)

		return err
	}

	//存在则更新
	sql := "update Node set NodeStatus=? where NodeIP=?"
	_, err = db.d.Exec(sql, NodeStatus, nodeip)

	return err
}
