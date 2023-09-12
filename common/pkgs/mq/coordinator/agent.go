package coordinator

import "gitlink.org.cn/cloudream/common/pkgs/mq"

type AgentService interface {
	TempCacheReport(msg *TempCacheReport)

	AgentStatusReport(msg *AgentStatusReport)
}

// 代理端发给协调端，告知临时缓存的数据
var _ = RegisterNoReply(Service.TempCacheReport)

type TempCacheReport struct {
	mq.MessageBodyBase
	NodeID int64    `json:"nodeID"`
	Hashes []string `json:"hashes"`
}

func NewTempCacheReportBody(nodeID int64, hashes []string) *TempCacheReport {
	return &TempCacheReport{
		NodeID: nodeID,
		Hashes: hashes,
	}
}
func (client *Client) TempCacheReport(msg *TempCacheReport) error {
	return mq.Send(AgentService.TempCacheReport, client.rabbitCli, msg)
}

// 代理端发给协调端，告知延迟、ipfs和资源目录的可达性
var _ = RegisterNoReply(Service.AgentStatusReport)

type AgentStatusReport struct {
	mq.MessageBodyBase
	NodeID         int64   `json:"nodeID"`
	NodeDelayIDs   []int64 `json:"nodeDelayIDs"`
	NodeDelays     []int   `json:"nodeDelays"`
	IPFSStatus     string  `json:"ipfsStatus"`
	LocalDirStatus string  `json:"localDirStatus"`
}

func NewAgentStatusReportBody(nodeID int64, nodeDelayIDs []int64, nodeDelays []int, ipfsStatus string, localDirStatus string) *AgentStatusReport {
	return &AgentStatusReport{
		NodeID:         nodeID,
		NodeDelayIDs:   nodeDelayIDs,
		NodeDelays:     nodeDelays,
		IPFSStatus:     ipfsStatus,
		LocalDirStatus: localDirStatus,
	}
}
func (client *Client) AgentStatusReport(msg *AgentStatusReport) error {
	return mq.Send(AgentService.AgentStatusReport, client.rabbitCli, msg)
}
