package config

import (
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/utils/config"
	stgmodels "gitlink.org.cn/cloudream/storage/common/models"
	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
	stgmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq"
)

type Config struct {
	Local       stgmodels.LocalMachineInfo `json:"local"`
	AgentGRPC   agtrpc.PoolConfig          `json:"agentGRPC"`
	MaxRepCount int                        `json:"maxRepCount"`
	Logger      logger.Config              `json:"logger"`
	RabbitMQ    stgmq.Config               `json:"rabbitMQ"`
	IPFS        *ipfs.Config               `json:"ipfs"` // 此字段非空代表客户端上存在ipfs daemon
	DistLock    distlock.Config            `json:"distlock"`
}

var cfg Config

func Init() error {
	return config.DefaultLoad("client", &cfg)
}

func Cfg() *Config {
	return &cfg
}
