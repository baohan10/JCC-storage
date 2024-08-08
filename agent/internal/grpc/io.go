package grpc

import (
	"context"
	"fmt"
	"io"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/serder"
	agtrpc "gitlink.org.cn/cloudream/storage/common/pkgs/grpc/agent"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

func (s *Service) ExecuteIOPlan(ctx context.Context, req *agtrpc.ExecuteIOPlanReq) (*agtrpc.ExecuteIOPlanResp, error) {
	plan, err := serder.JSONToObjectEx[ioswitch.Plan]([]byte(req.Plan))
	if err != nil {
		return nil, fmt.Errorf("deserializing plan: %w", err)
	}

	logger.WithField("PlanID", plan.ID).Infof("begin execute io plan")
	defer logger.WithField("PlanID", plan.ID).Infof("plan finished")

	sw := ioswitch.NewSwitch(plan)

	s.swMgr.Add(sw)
	defer s.swMgr.Remove(sw)

	err = sw.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("running io plan: %w", err)
	}

	return &agtrpc.ExecuteIOPlanResp{}, nil
}

func (s *Service) SendStream(server agtrpc.Agent_SendStreamServer) error {
	msg, err := server.Recv()
	if err != nil {
		return fmt.Errorf("recving stream id packet: %w", err)
	}
	if msg.Type != agtrpc.StreamDataPacketType_SendArgs {
		return fmt.Errorf("first packet must be a SendArgs packet")
	}

	logger.
		WithField("PlanID", msg.PlanID).
		WithField("VarID", msg.VarID).
		Debugf("receive stream")

	// 同一批Plan中每个节点的Plan的启动时间有先后，但最多不应该超过30秒
	ctx, cancel := context.WithTimeout(server.Context(), time.Second*30)
	defer cancel()

	sw := s.swMgr.FindByIDContexted(ctx, ioswitch.PlanID(msg.PlanID))
	if sw == nil {
		return fmt.Errorf("plan not found")
	}

	pr, pw := io.Pipe()

	varID := ioswitch.VarID(msg.VarID)
	sw.PutVars(&ioswitch.StreamVar{
		ID:     varID,
		Stream: pr,
	})

	// 然后读取文件数据
	var recvSize int64
	for {
		msg, err := server.Recv()

		// 读取客户端数据失败
		// 即使err是io.EOF，只要没有收到客户端包含EOF数据包就被断开了连接，就认为接收失败
		if err != nil {
			// 关闭文件写入
			pw.CloseWithError(io.ErrClosedPipe)
			logger.WithField("ReceiveSize", recvSize).
				WithField("VarID", varID).
				Warnf("recv message failed, err: %s", err.Error())
			return fmt.Errorf("recv message failed, err: %w", err)
		}

		err = io2.WriteAll(pw, msg.Data)
		if err != nil {
			// 关闭文件写入
			pw.CloseWithError(io.ErrClosedPipe)
			logger.Warnf("write data to file failed, err: %s", err.Error())
			return fmt.Errorf("write data to file failed, err: %w", err)
		}

		recvSize += int64(len(msg.Data))

		if msg.Type == agtrpc.StreamDataPacketType_EOF {
			// 客户端明确说明文件传输已经结束，那么结束写入，获得文件Hash
			err := pw.Close()
			if err != nil {
				logger.Warnf("finish writing failed, err: %s", err.Error())
				return fmt.Errorf("finish writing failed, err: %w", err)
			}

			// 并将结果返回到客户端
			err = server.SendAndClose(&agtrpc.SendStreamResp{})
			if err != nil {
				logger.Warnf("send response failed, err: %s", err.Error())
				return fmt.Errorf("send response failed, err: %w", err)
			}

			return nil
		}
	}
}

func (s *Service) GetStream(req *agtrpc.GetStreamReq, server agtrpc.Agent_GetStreamServer) error {
	logger.
		WithField("PlanID", req.PlanID).
		WithField("VarID", req.VarID).
		Debugf("send stream")

	// 同上
	ctx, cancel := context.WithTimeout(server.Context(), time.Second*30)
	defer cancel()

	sw := s.swMgr.FindByIDContexted(ctx, ioswitch.PlanID(req.PlanID))
	if sw == nil {
		return fmt.Errorf("plan not found")
	}

	signal, err := serder.JSONToObjectEx[*ioswitch.SignalVar]([]byte(req.Signal))
	if err != nil {
		return fmt.Errorf("deserializing var: %w", err)
	}

	sw.PutVars(signal)

	strVar := &ioswitch.StreamVar{
		ID: ioswitch.VarID(req.VarID),
	}
	err = sw.BindVars(server.Context(), strVar)
	if err != nil {
		return fmt.Errorf("binding vars: %w", err)
	}

	reader := strVar.Stream
	defer reader.Close()

	buf := make([]byte, 1024*64)
	readAllCnt := 0
	for {
		readCnt, err := reader.Read(buf)

		if readCnt > 0 {
			readAllCnt += readCnt
			err = server.Send(&agtrpc.StreamDataPacket{
				Type: agtrpc.StreamDataPacketType_Data,
				Data: buf[:readCnt],
			})
			if err != nil {
				logger.
					WithField("PlanID", req.PlanID).
					WithField("VarID", req.VarID).
					Warnf("send stream data failed, err: %s", err.Error())
				return fmt.Errorf("send stream data failed, err: %w", err)
			}
		}

		// 文件读取完毕
		if err == io.EOF {
			logger.
				WithField("PlanID", req.PlanID).
				WithField("VarID", req.VarID).
				Debugf("send data size %d", readAllCnt)
			// 发送EOF消息
			server.Send(&agtrpc.StreamDataPacket{
				Type: agtrpc.StreamDataPacketType_EOF,
			})
			return nil
		}

		// io.ErrUnexpectedEOF没有读满整个buf就遇到了EOF，此时正常发送剩余数据即可。除了这两个错误之外，其他错误都中断操作
		if err != nil && err != io.ErrUnexpectedEOF {
			logger.
				WithField("PlanID", req.PlanID).
				WithField("VarID", req.VarID).
				Warnf("reading stream data: %s", err.Error())
			return fmt.Errorf("reading stream data: %w", err)
		}
	}
}

func (s *Service) SendVar(ctx context.Context, req *agtrpc.SendVarReq) (*agtrpc.SendVarResp, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	sw := s.swMgr.FindByIDContexted(ctx, ioswitch.PlanID(req.PlanID))
	if sw == nil {
		return nil, fmt.Errorf("plan not found")
	}

	v, err := serder.JSONToObjectEx[ioswitch.Var]([]byte(req.Var))
	if err != nil {
		return nil, fmt.Errorf("deserializing var: %w", err)
	}

	sw.PutVars(v)
	return &agtrpc.SendVarResp{}, nil
}

func (s *Service) GetVar(ctx context.Context, req *agtrpc.GetVarReq) (*agtrpc.GetVarResp, error) {
	ctx2, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	sw := s.swMgr.FindByIDContexted(ctx2, ioswitch.PlanID(req.PlanID))
	if sw == nil {
		return nil, fmt.Errorf("plan not found")
	}

	v, err := serder.JSONToObjectEx[ioswitch.Var]([]byte(req.Var))
	if err != nil {
		return nil, fmt.Errorf("deserializing var: %w", err)
	}

	signal, err := serder.JSONToObjectEx[*ioswitch.SignalVar]([]byte(req.Signal))
	if err != nil {
		return nil, fmt.Errorf("deserializing var: %w", err)
	}

	sw.PutVars(signal)

	err = sw.BindVars(ctx, v)
	if err != nil {
		return nil, fmt.Errorf("binding vars: %w", err)
	}

	vd, err := serder.ObjectToJSONEx(v)
	if err != nil {
		return nil, fmt.Errorf("serializing var: %w", err)
	}

	return &agtrpc.GetVarResp{
		Var: string(vd),
	}, nil
}
