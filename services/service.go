package services

import mydb "gitlink.org.cn/cloudream/db"

type Service struct {
	db *mydb.DB
}

func NewService(db *mydb.DB) *Service {
	return &Service{
		db: db,
	}
}
