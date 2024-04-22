package mq

import (
	mydb "gitlink.org.cn/cloudream/storage/common/pkgs/db"
)

type Service struct {
	db *mydb.DB
}

func NewService(db *mydb.DB) *Service {
	return &Service{
		db: db,
	}
}
