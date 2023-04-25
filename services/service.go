package services

import (
	racli "gitlink.org.cn/cloudream/rabbitmq/client/coordinator"
)

type Service struct {
	coordinator *racli.CoordinatorClient
}

func NewService(coorClient *racli.CoordinatorClient) (*Service, error) {
	return &Service{
		coordinator: coorClient,
	}, nil
}
