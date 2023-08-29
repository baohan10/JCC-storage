package agent

import (
	"context"
	"fmt"
	"io"

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
				Type: FileDataPacketType_EOF,
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
			Type: FileDataPacketType_Data,
			Data: buf[:rd],
		})
		if err != nil {
			return "", fmt.Errorf("sending data packet: %w", err)
		}
	}
}

type fileReadCloser struct {
	io.ReadCloser
	stream      Agent_GetIPFSFileClient
	cancelFn    context.CancelFunc
	readingData []byte
	recvEOF     bool
}

func (s *fileReadCloser) Read(p []byte) (int, error) {
	if len(s.readingData) == 0 && !s.recvEOF {
		resp, err := s.stream.Recv()
		if err != nil {
			return 0, err
		}

		if resp.Type == FileDataPacketType_Data {
			s.readingData = resp.Data

		} else if resp.Type == FileDataPacketType_EOF {
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
		stream:   stream,
		cancelFn: cancel,
	}, nil
}

func (c *Client) Close() {
	c.con.Close()
}
