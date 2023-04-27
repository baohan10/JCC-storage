package main

import (
	"io"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	"gitlink.org.cn/cloudream/agent/internal/config"
	"gitlink.org.cn/cloudream/utils"

	racli "gitlink.org.cn/cloudream/rabbitmq/client"
	ramsg "gitlink.org.cn/cloudream/rabbitmq/message"
	agtmsg "gitlink.org.cn/cloudream/rabbitmq/message/agent"
	coormsg "gitlink.org.cn/cloudream/rabbitmq/message/coordinator"
	"gitlink.org.cn/cloudream/utils/consts/errorcode"
	myio "gitlink.org.cn/cloudream/utils/io"
	"gitlink.org.cn/cloudream/utils/ipfs"
)

type CommandService struct {
	ipfs *ipfs.IPFS
}

func NewCommandService(ipfs *ipfs.IPFS) *CommandService {
	return &CommandService{
		ipfs: ipfs,
	}
}

func (service *CommandService) RepMove(msg *agtmsg.RepMoveCommand) *agtmsg.AgentMoveResp {
	outFileName := utils.MakeMoveOperationFileName(msg.Body.ObjectID, msg.Body.UserID)
	outFileDir := filepath.Join(config.Cfg().StorageBaseDir, msg.Body.Directory)
	outFilePath := filepath.Join(outFileDir, outFileName)

	err := os.MkdirAll(outFileDir, 0644)
	if err != nil {
		log.Warnf("create file directory %s failed, err: %s", outFileDir, err.Error())
		return ramsg.ReplyFailed[agtmsg.AgentMoveResp](errorcode.OPERATION_FAILED, "create local file directory failed")
	}

	outFile, err := os.Create(outFilePath)
	if err != nil {
		log.Warnf("create file %s failed, err: %s", outFilePath, err.Error())
		return ramsg.ReplyFailed[agtmsg.AgentMoveResp](errorcode.OPERATION_FAILED, "create local file failed")
	}
	defer outFile.Close()

	hashs := msg.Body.Hashs
	fileHash := hashs[0]
	ipfsRd, err := service.ipfs.OpenRead(fileHash)
	if err != nil {
		log.Warnf("read ipfs file %s failed, err: %s", fileHash, err.Error())
		return ramsg.ReplyFailed[agtmsg.AgentMoveResp](errorcode.OPERATION_FAILED, "read ipfs file failed")
	}
	defer ipfsRd.Close()

	buf := make([]byte, 1024)
	for {
		readCnt, err := ipfsRd.Read(buf)

		// 文件读取完毕
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Warnf("read ipfs file %s data failed, err: %s", fileHash, err.Error())
			return ramsg.ReplyFailed[agtmsg.AgentMoveResp](errorcode.OPERATION_FAILED, "read ipfs file data failed")
		}

		err = myio.WriteAll(outFile, buf[:readCnt])
		if err != nil {
			log.Warnf("write data to file %s failed, err: %s", outFilePath, err.Error())
			return ramsg.ReplyFailed[agtmsg.AgentMoveResp](errorcode.OPERATION_FAILED, "write data to file failed")
		}
	}

	//向coor报告临时缓存hash
	coorClient, err := racli.NewCoordinatorClient()
	if err != nil {
		log.Warnf("new coordinator client failed, err: %s", err.Error())
		return ramsg.ReplyFailed[agtmsg.AgentMoveResp](errorcode.OPERATION_FAILED, "new coordinator client failed")
	}
	defer coorClient.Close()

	// TODO 这里更新失败残留下的文件是否要删除？
	coorClient.TempCacheReport(coormsg.NewTempCacheReportBody(config.Cfg().ID, hashs))

	return ramsg.ReplyOK(agtmsg.NewAgentMoveRespBody())
}
