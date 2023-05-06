package config

import (
	racfg "gitlink.org.cn/cloudream/rabbitmq/config"
	"gitlink.org.cn/cloudream/utils/config"
	"gitlink.org.cn/cloudream/utils/ipfs"
)

type Config struct {
	GRPCPort           int          `json:"grpcPort"`
	GRCPPacketSize     int64        `json:"grpcPacketSize"`
	MaxReplicateNumber int          `json:"maxReplicateNumber"`
	LocalIP            string       `json:"localIP"`
	ExternalIP         string       `json:"externalIP"`
	RabbitMQ           racfg.Config `json:"rabbitMQ"`
	IPFS               *ipfs.Config `json:"ipfs"` // 此字段非空代表客户端上存在ipfs daemon
}

var cfg Config

func Init() error {
	return config.DefaultLoad("client", &cfg)
}

func Cfg() *Config {
	return &cfg
}
