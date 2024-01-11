package mq

import (
	mydb "gitlink.org.cn/cloudream/storage/common/pkgs/db"
	scmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner"
)

type Service struct {
	db      *mydb.DB
	scanner *scmq.Client
}

func NewService(db *mydb.DB, scanner *scmq.Client) *Service {
	return &Service{
		db:      db,
		scanner: scanner,
	}
}
