package db

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
	"gitlink.org.cn/cloudream/common/utils/math"
)

const (
	maxPlaceholderCount = 65535
)

func BatchNamedExec[T any](ctx SQLContext, sql string, argCnt int, arr []T, callback func(sql.Result) bool) error {
	if argCnt == 0 {
		ret, err := ctx.NamedExec(sql, arr)
		if err != nil {
			return err
		}

		if callback != nil {
			callback(ret)
		}

		return nil
	}

	batchSize := maxPlaceholderCount / argCnt
	for len(arr) > 0 {
		curBatchSize := math.Min(batchSize, len(arr))

		ret, err := ctx.NamedExec(sql, arr[:curBatchSize])
		if err != nil {
			return err
		}
		if callback != nil && !callback(ret) {
			return nil
		}

		arr = arr[curBatchSize:]
	}

	return nil
}

func BatchNamedQuery[T any](ctx SQLContext, sql string, argCnt int, arr []T, callback func(*sqlx.Rows) bool) error {
	if argCnt == 0 {
		ret, err := ctx.NamedQuery(sql, arr)
		if err != nil {
			return err
		}

		if callback != nil {
			callback(ret)
		}

		return nil
	}

	batchSize := maxPlaceholderCount / argCnt
	for len(arr) > 0 {
		curBatchSize := math.Min(batchSize, len(arr))

		ret, err := ctx.NamedQuery(sql, arr[:curBatchSize])
		if err != nil {
			return err
		}
		if callback != nil && !callback(ret) {
			return nil
		}

		arr = arr[curBatchSize:]
	}
	return nil
}
