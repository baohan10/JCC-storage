package config

import (
	racfg "gitlink.org.cn/cloudream/rabbitmq/config"
	"gitlink.org.cn/cloudream/utils/config"
)

type Config struct {
	GRPCPort           int          `json:"grpcPort"`
	GRCPPacketSize     int64        `json:"grpcPacketSize"`
	IPFSPort           int          `json:"ipfsPort"`
	MaxReplicateNumber int          `json:"maxReplicateNumber"`
	LocalIP            string       `json:"localIP"`
	ExternalIP         string       `json:"externalIP"`
	RabbitMQ           racfg.Config `json:"rabbitMQ"`
}

var cfg Config

func Init() error {
	return config.DefaultLoad("client", &cfg)
}

func Cfg() *Config {
	return &cfg
}
