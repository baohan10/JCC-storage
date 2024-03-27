package config

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	c "gitlink.org.cn/cloudream/common/utils/config"
	stgmodels "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	"gitlink.org.cn/cloudream/storage/common/pkgs/grpc"
	stgmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
)

type Config struct {
	ID               int64                      `json:"id"`
	Local            stgmodels.LocalMachineInfo `json:"local"`
	GRPC             *grpc.Config               `json:"grpc"`
	TempFileLifetime int                        `json:"tempFileLifetime"` // temp状态的副本最多能保持多久时间，单位：秒
	Logger           log.Config                 `json:"logger"`
	RabbitMQ         stgmq.Config               `json:"rabbitMQ"`
	IPFS             ipfs.Config                `json:"ipfs"`
	DistLock         distlock.Config            `json:"distlock"`
	Connectivity     connectivity.Config        `json:"connectivity"`
}

var cfg Config

func Init() error {
	return c.DefaultLoad("agent", &cfg)
}

func Cfg() *Config {
	return &cfg
}
