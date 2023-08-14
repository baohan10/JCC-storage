package services

import (
	mydb "gitlink.org.cn/cloudream/storage-common/pkgs/db"
	sccli "gitlink.org.cn/cloudream/storage-common/pkgs/mq/client/scanner"
)

type Service struct {
	db      *mydb.DB
	scanner *sccli.Client
}

func NewService(db *mydb.DB, scanner *sccli.Client) *Service {
	return &Service{
		db:      db,
		scanner: scanner,
	}
}
