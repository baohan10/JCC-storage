package services

import (
	distlock "gitlink.org.cn/cloudream/common/pkg/distlock/service"
	mydb "gitlink.org.cn/cloudream/db"
	sccli "gitlink.org.cn/cloudream/rabbitmq/client/scanner"
)

type Service struct {
	db       *mydb.DB
	scanner  *sccli.Client
	distlock *distlock.Service
}

func NewService(db *mydb.DB, scanner *sccli.Client, distlock *distlock.Service) *Service {
	return &Service{
		db:       db,
		scanner:  scanner,
		distlock: distlock,
	}
}
