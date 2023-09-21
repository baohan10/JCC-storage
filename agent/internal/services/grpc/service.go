package grpc

import (
	"fmt"
	"io"

	log "gitlink.org.cn/cloudream/common/pkgs/logger"
	myio "gitlink.org.cn/cloudream/common/utils/io"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	agentserver "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
)

type Service struct {
	agentserver.AgentServer
}

func NewService() *Service {
	return &Service{}
}

func (s *Service) SendIPFSFile(server agentserver.Agent_SendIPFSFileServer) error {
	log.Debugf("client upload file")

	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		log.Warnf("new ipfs client: %s", err.Error())
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer ipfsCli.Close()

	writer, err := ipfsCli.CreateFileStream()
	if err != nil {
		log.Warnf("create file failed, err: %s", err.Error())
		return fmt.Errorf("create file failed, err: %w", err)
	}

	// 然后读取文件数据
	var recvSize int64
	for {
		msg, err := server.Recv()

		// 读取客户端数据失败
		// 即使err是io.EOF，只要没有收到客户端包含EOF数据包就被断开了连接，就认为接收失败
		if err != nil {
			// 关闭文件写入，不需要返回的hash和error
			writer.Abort(io.ErrClosedPipe)
			log.WithField("ReceiveSize", recvSize).
				Warnf("recv message failed, err: %s", err.Error())
			return fmt.Errorf("recv message failed, err: %w", err)
		}

		err = myio.WriteAll(writer, msg.Data)
		if err != nil {
			// 关闭文件写入，不需要返回的hash和error
			writer.Abort(io.ErrClosedPipe)
			log.Warnf("write data to file failed, err: %s", err.Error())
			return fmt.Errorf("write data to file failed, err: %w", err)
		}

		recvSize += int64(len(msg.Data))

		if msg.Type == agentserver.FileDataPacketType_EOF {
			// 客户端明确说明文件传输已经结束，那么结束写入，获得文件Hash
			hash, err := writer.Finish()
			if err != nil {
				log.Warnf("finish writing failed, err: %s", err.Error())
				return fmt.Errorf("finish writing failed, err: %w", err)
			}

			// 并将结果返回到客户端
			err = server.SendAndClose(&agentserver.SendIPFSFileResp{
				FileHash: hash,
			})
			if err != nil {
				log.Warnf("send response failed, err: %s", err.Error())
				return fmt.Errorf("send response failed, err: %w", err)
			}

			return nil
		}
	}
}

func (s *Service) GetIPFSFile(req *agentserver.GetIPFSFileReq, server agentserver.Agent_GetIPFSFileServer) error {
	log.WithField("FileHash", req.FileHash).Debugf("client download file")

	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		log.Warnf("new ipfs client: %s", err.Error())
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer ipfsCli.Close()

	reader, err := ipfsCli.OpenRead(req.FileHash)
	if err != nil {
		log.Warnf("open file %s to read failed, err: %s", req.FileHash, err.Error())
		return fmt.Errorf("open file to read failed, err: %w", err)
	}
	defer reader.Close()

	buf := make([]byte, 1024)
	readAllCnt := 0
	for {
		readCnt, err := reader.Read(buf)

		if readCnt > 0 {
			readAllCnt += readCnt
			err = server.Send(&agentserver.FileDataPacket{
				Type: agentserver.FileDataPacketType_Data,
				Data: buf[:readCnt],
			})
			if err != nil {
				log.WithField("FileHash", req.FileHash).
					Warnf("send file data failed, err: %s", err.Error())
				return fmt.Errorf("send file data failed, err: %w", err)
			}
		}

		// 文件读取完毕
		if err == io.EOF {
			log.WithField("FileHash", req.FileHash).Debugf("send data size %d", readAllCnt)
			// 发送EOF消息
			server.Send(&agentserver.FileDataPacket{
				Type: agentserver.FileDataPacketType_EOF,
			})
			return nil
		}

		// io.ErrUnexpectedEOF没有读满整个buf就遇到了EOF，此时正常发送剩余数据即可。除了这两个错误之外，其他错误都中断操作
		if err != nil && err != io.ErrUnexpectedEOF {
			log.Warnf("read file %s data failed, err: %s", req.FileHash, err.Error())
			return fmt.Errorf("read file data failed, err: %w", err)
		}
	}
}
