package config

import (
	"gitlink.org.cn/cloudream/common/pkg/distlock"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	"gitlink.org.cn/cloudream/common/utils/config"
	"gitlink.org.cn/cloudream/common/utils/ipfs"
	racfg "gitlink.org.cn/cloudream/rabbitmq/config"
)

type Config struct {
	GRPCPort       int             `json:"grpcPort"`
	GRCPPacketSize int64           `json:"grpcPacketSize"`
	MaxRepCount    int             `json:"maxRepCount"`
	LocalIP        string          `json:"localIP"`
	ExternalIP     string          `json:"externalIP"`
	Logger         logger.Config   `json:"logger"`
	RabbitMQ       racfg.Config    `json:"rabbitMQ"`
	IPFS           *ipfs.Config    `json:"ipfs"` // 此字段非空代表客户端上存在ipfs daemon
	DistLock       distlock.Config `json:"distlock"`
}

var cfg Config

func Init() error {
	return config.DefaultLoad("client", &cfg)
}

func Cfg() *Config {
	return &cfg
}
