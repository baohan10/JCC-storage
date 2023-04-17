package main

import (
	"fmt"
	"io"

	log "github.com/sirupsen/logrus"
	"gitlink.org.cn/cloudream/agent/config"
	agentserver "gitlink.org.cn/cloudream/proto"
	myio "gitlink.org.cn/cloudream/utils/io"
	"gitlink.org.cn/cloudream/utils/ipfs"
)

type GRPCService struct {
	agentserver.TranBlockOrReplicaServer
	ipfs *ipfs.IPFS
}

func NewGPRCService(ipfs *ipfs.IPFS) *GRPCService {
	return &GRPCService{
		ipfs: ipfs,
	}
}

func (s *GRPCService) SendBlockOrReplica(server agentserver.TranBlockOrReplica_SendBlockOrReplicaServer) error {
	writer, err := s.ipfs.CreateFile()
	if err != nil {
		log.Warnf("create file failed, err: %s", err.Error())
		return fmt.Errorf("create file failed, err: %w", err)
	}

	for {
		msg, err := server.Recv()

		// 客户端数据发送完毕，则停止文件写入，获得文件Hash
		if err == io.EOF {
			hash, err := writer.Finish(io.EOF)
			if err != nil {
				log.Warnf("finish writing failed, err: %s", err.Error())
				return fmt.Errorf("finish writing failed, err: %w", err)
			}

			// 并将结果返回到客户端
			server.SendAndClose(&agentserver.SendRes{
				BlockOrReplicaName: msg.BlockOrReplicaName,
				BlockOrReplicaHash: hash,
			})
			return nil
		}

		// 读取客户端数据失败
		if err != nil {
			// 关闭文件写入，不需要返回的hash和error
			writer.Finish(io.ErrClosedPipe)
			log.Warnf("recv message failed, err: %s", err.Error())
			return fmt.Errorf("recv message failed, err: %w", err)
		}

		// 写入到文件失败
		err = myio.WriteAll(writer, msg.BlockOrReplicaData)
		if err != nil {
			// 关闭文件写入，不需要返回的hash和error
			writer.Finish(io.ErrClosedPipe)
			log.Warnf("write data to file failed, err: %s", err.Error())
			return fmt.Errorf("write data to file failed, err: %w", err)
		}
	}
}

func (s *GRPCService) GetBlockOrReplica(req *agentserver.GetReq, server agentserver.TranBlockOrReplica_GetBlockOrReplicaServer) error {
	reader, err := s.ipfs.OpenRead(req.BlockOrReplicaHash)
	if err != nil {
		log.Warnf("open file %s to read failed, err: %s", req.BlockOrReplicaHash, err.Error())
		return fmt.Errorf("open file to read failed, err: %w", err)
	}
	defer reader.Close()

	buf := make([]byte, 0, config.Cfg().GRCPPacketSize)
	for {
		readCnt, err := io.ReadFull(reader, buf)

		// 文件读取完毕
		if err == io.EOF {
			return nil
		}

		// io.ErrUnexpectedEOF没有读满整个buf就遇到了EOF，此时正常发送剩余数据即可。除了这两个错误之外，其他错误都中断操作
		if err != io.ErrUnexpectedEOF {
			log.Warnf("read file %s data failed, err: %s", req.BlockOrReplicaHash, err.Error())
			return fmt.Errorf("read file data failed, err: %w", err)
		}

		server.Send(&agentserver.BlockOrReplica{
			BlockOrReplicaName: "json",
			BlockOrReplicaHash: "json",
			BlockOrReplicaData: buf[:readCnt],
		})
	}
	return nil
}
