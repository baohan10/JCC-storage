package services

import (
	mydb "gitlink.org.cn/cloudream/db"
	sccli "gitlink.org.cn/cloudream/rabbitmq/client/scanner"
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
