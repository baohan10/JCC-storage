package db

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/config"
)

type DB struct {
	d *sqlx.DB
}

type SQLContext interface {
	sqlx.Queryer
	sqlx.Execer
	sqlx.Ext
	sqlx.Preparer

	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	NamedExec(query string, arg interface{}) (sql.Result, error)
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
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
