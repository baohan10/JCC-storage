package globals

import (
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	agtrpc "gitlink.org.cn/cloudream/storage-common/pkgs/grpc/agent"
	stgmq "gitlink.org.cn/cloudream/storage-common/pkgs/mq"
	agtmq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
	scmq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/scanner"
)

var AgentMQPool *agtmq.Pool

var CoordinatorMQPool *coormq.Pool

var ScannerMQPool *scmq.Pool

func InitMQPool(cfg *stgmq.Config) {
	AgentMQPool = agtmq.NewPool(cfg)

	CoordinatorMQPool = coormq.NewPool(cfg)

	ScannerMQPool = scmq.NewPool(cfg)
}

var AgentRPCPool *agtrpc.Pool

func InitAgentRPCPool(cfg *agtrpc.PoolConfig) {
	AgentRPCPool = agtrpc.NewPool(cfg)
}

var IPFSPool *ipfs.Pool

func InitIPFSPool(cfg *ipfs.Config) {
	IPFSPool = ipfs.NewPool(cfg)
}
