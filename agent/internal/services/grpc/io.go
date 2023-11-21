package grpc

import (
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	myio "gitlink.org.cn/cloudream/common/utils/io"
	agentserver "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

func (s *Service) SendStream(server agentserver.Agent_SendStreamServer) error {
	msg, err := server.Recv()
	if err != nil {
		return fmt.Errorf("recving stream id packet: %w", err)
	}
	if msg.Type != agentserver.StreamDataPacketType_SendArgs {
		return fmt.Errorf("first packet must be a SendArgs packet")
	}

	logger.
		WithField("PlanID", msg.PlanID).
		WithField("StreamID", msg.StreamID).
		Debugf("receive stream from grpc")

	pr, pw := io.Pipe()

	s.sw.StreamReady(ioswitch.PlanID(msg.PlanID), ioswitch.NewStream(ioswitch.StreamID(msg.StreamID), pr))

	// 然后读取文件数据
	var recvSize int64
	for {
		msg, err := server.Recv()

		// 读取客户端数据失败
		// 即使err是io.EOF，只要没有收到客户端包含EOF数据包就被断开了连接，就认为接收失败
		if err != nil {
			// 关闭文件写入，不需要返回的hash和error
			pw.CloseWithError(io.ErrClosedPipe)
			logger.WithField("ReceiveSize", recvSize).
				Warnf("recv message failed, err: %s", err.Error())
			return fmt.Errorf("recv message failed, err: %w", err)
		}

		err = myio.WriteAll(pw, msg.Data)
		if err != nil {
			// 关闭文件写入，不需要返回的hash和error
			pw.CloseWithError(io.ErrClosedPipe)
			logger.Warnf("write data to file failed, err: %s", err.Error())
			return fmt.Errorf("write data to file failed, err: %w", err)
		}

		recvSize += int64(len(msg.Data))

		if msg.Type == agentserver.StreamDataPacketType_EOF {
			// 客户端明确说明文件传输已经结束，那么结束写入，获得文件Hash
			err := pw.Close()
			if err != nil {
				logger.Warnf("finish writing failed, err: %s", err.Error())
				return fmt.Errorf("finish writing failed, err: %w", err)
			}

			// 并将结果返回到客户端
			err = server.SendAndClose(&agentserver.SendStreamResp{})
			if err != nil {
				logger.Warnf("send response failed, err: %s", err.Error())
				return fmt.Errorf("send response failed, err: %w", err)
			}

			return nil
		}
	}
}

func (s *Service) FetchStream(req *agentserver.FetchStreamReq, server agentserver.Agent_FetchStreamServer) error {
	logger.
		WithField("PlanID", req.PlanID).
		WithField("StreamID", req.StreamID).
		Debugf("send stream by grpc")

	strs, err := s.sw.WaitStreams(ioswitch.PlanID(req.PlanID), ioswitch.StreamID(req.StreamID))
	if err != nil {
		logger.
			WithField("PlanID", req.PlanID).
			WithField("StreamID", req.StreamID).
			Warnf("watting stream: %s", err.Error())
		return fmt.Errorf("watting stream: %w", err)
	}

	reader := strs[0].Stream
	defer reader.Close()

	buf := make([]byte, 4096)
	readAllCnt := 0
	for {
		readCnt, err := reader.Read(buf)

		if readCnt > 0 {
			readAllCnt += readCnt
			err = server.Send(&agentserver.StreamDataPacket{
				Type: agentserver.StreamDataPacketType_Data,
				Data: buf[:readCnt],
			})
			if err != nil {
				logger.
					WithField("PlanID", req.PlanID).
					WithField("StreamID", req.StreamID).
					Warnf("send stream data failed, err: %s", err.Error())
				return fmt.Errorf("send stream data failed, err: %w", err)
			}
		}

		// 文件读取完毕
		if err == io.EOF {
			logger.
				WithField("PlanID", req.PlanID).
				WithField("StreamID", req.StreamID).
				Debugf("send data size %d", readAllCnt)
			// 发送EOF消息
			server.Send(&agentserver.StreamDataPacket{
				Type: agentserver.StreamDataPacketType_EOF,
			})
			return nil
		}

		// io.ErrUnexpectedEOF没有读满整个buf就遇到了EOF，此时正常发送剩余数据即可。除了这两个错误之外，其他错误都中断操作
		if err != nil && err != io.ErrUnexpectedEOF {
			logger.
				WithField("PlanID", req.PlanID).
				WithField("StreamID", req.StreamID).
				Warnf("reading stream data: %s", err.Error())
			return fmt.Errorf("reading stream data: %w", err)
		}
	}
}
