package services

import (
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
)

func (service *Service) ECWrite(msg *coormsg.ECWriteCommand) *coormsg.WriteResp {
	panic("not implement yet!")

	/*
		//jh：根据command中的UserId查询用户节点权限表，返回用户可用的NodeIp
		//kx：根据command中的ecName，得到ecN，然后从jh查到的NodeIp中选择ecN个，赋值给Ips
		//jh：完成对象表、对象编码块表的插入（对象编码块表的Hash字段先不插入）
		//返回消息
		//查询用户可用的节点IP
		nodes, err := service.db.QueryUserNodes(msg.Body.UserID)
		if err != nil {
			log.Warnf("query user nodes failed, err: %s", err.Error())
			return ramsg.NewCoorWriteRespFailed(errorcode.OPERATION_FAILED, "query user nodes failed")
		}

		ecid := msg.Body.ECName
		ecPolicies := *utils.GetEcPolicy()
		ecPolicy := ecPolicies[ecid]
		ecN := ecPolicy.GetN()

		if len(nodes) < ecN {
			log.Warnf("user nodes are not enough, err: %s", err.Error())
			return ramsg.NewCoorWriteRespFailed(errorcode.OPERATION_FAILED, "user nodes are not enough")
		}

		ids := make([]int, ecN)
		ips := make([]string, ecN)
		//随机选取numRep个nodeIp
		start := utils.GetRandInt(len(nodes))
		for i := 0; i < ecN; i++ {
			index := (start + i) % len(nodes)
			ids[i] = nodes[index].NodeID
			ips[i] = nodes[index].IP
		}

		// TODO 参考RepWrite，将创建EC对象的逻辑移动到WriteECHash中，并合成成一个事务
		//根据BucketName查询BucketID
		BucketID := Query_BucketID(msg.Body.BucketName)
		if BucketID == -1 {
			// TODO 日志
			return ramsg.NewCoorWriteRespFailed(errorcode.OPERATION_FAILED, fmt.Sprintf("bucket id not found for %s", msg.Body.BucketName))
		}
		//对象表插入Insert_Cache
		ObjectID := Insert_EcObject(msg.Body.ObjectName, BucketID, msg.Body.FileSizeInBytes, msg.Body.ECName)
		//对象编码块表插入，hash暂时为空
		for i := 0; i < ecN; i++ {
			Insert_EcObjectBlock(ObjectID, i)
		}
		return ramsg.NewCoorWriteRespOK(ids, ips)
	*/
}

func (service *Service) WriteECHash(msg *coormsg.WriteECHashCommand) *coormsg.WriteHashResp {
	panic("not implement yet!")

	/*
		//jh：根据command中的信息，插入对象编码块表中的Hash字段，并完成缓存表的插入
		//返回消息
		//插入对象编码块表中的Hash字段
		// TODO 参考WriteRepHash的逻辑
		ObjectId := Query_ObjectID(msg.Body.ObjectName)
		Insert_EcHash(ObjectId, msg.Body.Hashes)
		//缓存表的插入
		Insert_Cache(msg.Body.Hashes, msg.Body.NodeIDs, false)

		return ramsg.NewCoorWriteHashRespOK()
	*/
}
