package agent

import (
	"context"
	"fmt"
	"io"

	"gitlink.org.cn/cloudream/common/utils/serder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	con *grpc.ClientConn
	cli AgentClient
}

func NewClient(addr string) (*Client, error) {
	con, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &Client{
		con: con,
		cli: NewAgentClient(con),
	}, nil
}

func (c *Client) ExecuteIOPlan(ctx context.Context, plan ioswitch.Plan) error {
	data, err := serder.ObjectToJSONEx(plan)
	if err != nil {
		return err
	}

	_, err = c.cli.ExecuteIOPlan(ctx, &ExecuteIOPlanReq{
		Plan: string(data),
	})
	return err
}

type grpcStreamReadCloser struct {
	io.ReadCloser
	stream      Agent_GetStreamClient
	cancelFn    context.CancelFunc
	readingData []byte
	recvEOF     bool
}

func (s *grpcStreamReadCloser) Read(p []byte) (int, error) {
	if len(s.readingData) == 0 && !s.recvEOF {
		resp, err := s.stream.Recv()
		if err != nil {
			return 0, err
		}

		if resp.Type == StreamDataPacketType_Data {
			s.readingData = resp.Data

		} else if resp.Type == StreamDataPacketType_EOF {
			s.readingData = resp.Data
			s.recvEOF = true

		} else {
			return 0, fmt.Errorf("unsupported packt type: %v", resp.Type)
		}
	}

	cnt := copy(p, s.readingData)
	s.readingData = s.readingData[cnt:]

	if len(s.readingData) == 0 && s.recvEOF {
		return cnt, io.EOF
	}

	return cnt, nil
}

func (s *grpcStreamReadCloser) Close() error {
	s.cancelFn()

	return nil
}

func (c *Client) SendStream(ctx context.Context, planID ioswitch.PlanID, varID ioswitch.VarID, str io.Reader) error {
	sendCli, err := c.cli.SendStream(ctx)
	if err != nil {
		return err
	}

	err = sendCli.Send(&StreamDataPacket{
		Type:   StreamDataPacketType_SendArgs,
		PlanID: string(planID),
		VarID:  int32(varID),
	})
	if err != nil {
		return fmt.Errorf("sending first stream packet: %w", err)
	}

	buf := make([]byte, 1024*64)
	for {
		rd, err := str.Read(buf)
		if err == io.EOF {
			err := sendCli.Send(&StreamDataPacket{
				Type: StreamDataPacketType_EOF,
				Data: buf[:rd],
			})
			if err != nil {
				return fmt.Errorf("sending EOF packet: %w", err)
			}

			_, err = sendCli.CloseAndRecv()
			if err != nil {
				return fmt.Errorf("receiving response: %w", err)
			}

			return nil
		}

		if err != nil {
			return fmt.Errorf("reading stream data: %w", err)
		}

		err = sendCli.Send(&StreamDataPacket{
			Type: StreamDataPacketType_Data,
			Data: buf[:rd],
		})
		if err != nil {
			return fmt.Errorf("sending data packet: %w", err)
		}
	}
}

func (c *Client) GetStream(planID ioswitch.PlanID, varID ioswitch.VarID, signal *ioswitch.SignalVar) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(context.Background())

	sdata, err := serder.ObjectToJSONEx(signal)
	if err != nil {
		return nil, err
	}

	stream, err := c.cli.GetStream(ctx, &GetStreamReq{
		PlanID: string(planID),
		VarID:  int32(varID),
		Signal: string(sdata),
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("request grpc failed, err: %w", err)
	}

	return &grpcStreamReadCloser{
		stream:   stream,
		cancelFn: cancel,
	}, nil
}

func (c *Client) SendVar(ctx context.Context, planID ioswitch.PlanID, v ioswitch.Var) error {
	data, err := serder.ObjectToJSONEx(v)
	if err != nil {
		return err
	}

	_, err = c.cli.SendVar(ctx, &SendVarReq{
		PlanID: string(planID),
		Var:    string(data),
	})
	return err
}

func (c *Client) GetVar(ctx context.Context, planID ioswitch.PlanID, v ioswitch.Var, signal *ioswitch.SignalVar) (ioswitch.Var, error) {
	vdata, err := serder.ObjectToJSONEx(v)
	if err != nil {
		return nil, err
	}

	sdata, err := serder.ObjectToJSONEx(signal)
	if err != nil {
		return nil, err
	}

	resp, err := c.cli.GetVar(ctx, &GetVarReq{
		PlanID: string(planID),
		Var:    string(vdata),
		Signal: string(sdata),
	})
	if err != nil {
		return nil, err
	}

	v2, err := serder.JSONToObjectEx[ioswitch.Var]([]byte(resp.Var))
	if err != nil {
		return nil, err
	}

	return v2, nil
}

func (c *Client) Ping() error {
	_, err := c.cli.Ping(context.Background(), &PingReq{})
	return err
}

func (c *Client) Close() {
	c.con.Close()
}
