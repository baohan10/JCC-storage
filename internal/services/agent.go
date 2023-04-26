package services

import (
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
)

func (service *Service) TempCacheReport(msg *coormsg.TempCacheReport) {
	service.db.BatchInsertOrUpdateCache(msg.Hashes, msg.NodeID)
}

func (service *Service) AgentStatusReport(msg *coormsg.AgentStatusReport) {
	//jh：根据command中的Ip，插入节点延迟表，和节点表的NodeStatus
	//根据command中的Ip，插入节点延迟表

	// TODO
	/*
		ips := utils.GetAgentIps()
		Insert_NodeDelay(msg.IP, ips, msg.AgentDelay)

		//从配置表里读取节点地域NodeLocation
		//插入节点表的NodeStatus
		Insert_Node(msg.IP, msg.IP, msg.IPFSStatus, msg.LocalDirStatus)
	*/
}
