package coordinator

import "gitlink.org.cn/cloudream/common/pkgs/mq"

// 代理端发给协调端，告知临时缓存的数据
type TempCacheReport struct {
	NodeID int64    `json:"nodeID"`
	Hashes []string `json:"hashes"`
}

func NewTempCacheReportBody(nodeID int64, hashes []string) TempCacheReport {
	return TempCacheReport{
		NodeID: nodeID,
		Hashes: hashes,
	}
}

// 代理端发给协调端，告知延迟、ipfs和资源目录的可达性
type AgentStatusReport struct {
	NodeID         int64   `json:"nodeID"`
	NodeDelayIDs   []int64 `json:"nodeDelayIDs"`
	NodeDelays     []int   `json:"nodeDelays"`
	IPFSStatus     string  `json:"ipfsStatus"`
	LocalDirStatus string  `json:"localDirStatus"`
}

func NewAgentStatusReportBody(nodeID int64, nodeDelayIDs []int64, nodeDelays []int, ipfsStatus string, localDirStatus string) AgentStatusReport {
	return AgentStatusReport{
		NodeID:         nodeID,
		NodeDelayIDs:   nodeDelayIDs,
		NodeDelays:     nodeDelays,
		IPFSStatus:     ipfsStatus,
		LocalDirStatus: localDirStatus,
	}
}

func init() {
	mq.RegisterMessage[TempCacheReport]()

	mq.RegisterMessage[AgentStatusReport]()
}
