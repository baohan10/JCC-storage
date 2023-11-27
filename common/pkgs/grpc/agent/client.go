package agent

import (
	"context"
	"fmt"
	"io"

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

func (c *Client) SendIPFSFile(file io.Reader) (string, error) {
	sendCli, err := c.cli.SendIPFSFile(context.Background())
	if err != nil {
		return "", err
	}

	buf := make([]byte, 4096)
	for {
		rd, err := file.Read(buf)
		if err == io.EOF {
			err := sendCli.Send(&FileDataPacket{
				Type: StreamDataPacketType_EOF,
				Data: buf[:rd],
			})
			if err != nil {
				return "", fmt.Errorf("sending EOF packet: %w", err)
			}

			resp, err := sendCli.CloseAndRecv()
			if err != nil {
				return "", fmt.Errorf("receiving response: %w", err)
			}

			return resp.FileHash, nil
		}

		if err != nil {
			return "", fmt.Errorf("reading file data: %w", err)
		}

		err = sendCli.Send(&FileDataPacket{
			Type: StreamDataPacketType_Data,
			Data: buf[:rd],
		})
		if err != nil {
			return "", fmt.Errorf("sending data packet: %w", err)
		}
	}
}

type fileReadCloser struct {
	io.ReadCloser
	// stream      Agent_GetIPFSFileClient
	// TODO 临时使用
	recvFn      func() (*StreamDataPacket, error)
	cancelFn    context.CancelFunc
	readingData []byte
	recvEOF     bool
}

func (s *fileReadCloser) Read(p []byte) (int, error) {
	if len(s.readingData) == 0 && !s.recvEOF {
		resp, err := s.recvFn()
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

func (s *fileReadCloser) Close() error {
	s.cancelFn()

	return nil
}

func (c *Client) GetIPFSFile(fileHash string) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(context.Background())

	stream, err := c.cli.GetIPFSFile(ctx, &GetIPFSFileReq{
		FileHash: fileHash,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("request grpc failed, err: %w", err)
	}

	return &fileReadCloser{
		// TODO 临时处理方案
		recvFn: func() (*StreamDataPacket, error) {
			pkt, err := stream.Recv()
			if err != nil {
				return nil, err
			}

			return &StreamDataPacket{
				Type: pkt.Type,
				Data: pkt.Data,
			}, nil
		},
		cancelFn: cancel,
	}, nil
}

func (c *Client) SendStream(planID ioswitch.PlanID, streamID ioswitch.StreamID, file io.Reader) error {
	sendCli, err := c.cli.SendStream(context.Background())
	if err != nil {
		return err
	}

	err = sendCli.Send(&StreamDataPacket{
		Type:     StreamDataPacketType_SendArgs,
		PlanID:   string(planID),
		StreamID: string(streamID),
	})
	if err != nil {
		return fmt.Errorf("sending stream id packet: %w", err)
	}

	buf := make([]byte, 4096)
	for {
		rd, err := file.Read(buf)
		if err == io.EOF {
			err := sendCli.Send(&StreamDataPacket{
				Type:     StreamDataPacketType_EOF,
				StreamID: string(streamID),
				Data:     buf[:rd],
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
			return fmt.Errorf("reading file data: %w", err)
		}

		err = sendCli.Send(&StreamDataPacket{
			Type:     StreamDataPacketType_Data,
			StreamID: string(streamID),
			Data:     buf[:rd],
		})
		if err != nil {
			return fmt.Errorf("sending data packet: %w", err)
		}
	}
}

func (c *Client) FetchStream(planID ioswitch.PlanID, streamID ioswitch.StreamID) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(context.Background())

	stream, err := c.cli.FetchStream(ctx, &FetchStreamReq{
		PlanID:   string(planID),
		StreamID: string(streamID),
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("request grpc failed, err: %w", err)
	}

	return &fileReadCloser{
		recvFn:   stream.Recv,
		cancelFn: cancel,
	}, nil
}

func (c *Client) Close() {
	c.con.Close()
}
