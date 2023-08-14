package grpc

import (
	"context"
	"fmt"
	"io"

	myio "gitlink.org.cn/cloudream/common/utils/io"
	"gitlink.org.cn/cloudream/storage-common/pkgs/proto"
)

type fileReadCloser struct {
	io.ReadCloser
	stream   proto.FileTransport_GetFileClient
	cancelFn context.CancelFunc
	readData []byte
}

func (s *fileReadCloser) Read(p []byte) (int, error) {

	if s.readData == nil {
		resp, err := s.stream.Recv()
		if err != nil {
			return 0, err
		}

		if resp.Type == proto.FileDataPacketType_Data {
			s.readData = resp.Data

		} else if resp.Type == proto.FileDataPacketType_EOF {
			return 0, io.EOF

		} else {
			return 0, fmt.Errorf("unsuppoted packt type: %v", resp.Type)
		}
	}

	cnt := copy(p, s.readData)

	if len(s.readData) == cnt {
		s.readData = nil
	} else {
		s.readData = s.readData[cnt:]
	}

	return cnt, nil
}

func (s *fileReadCloser) Close() error {
	s.cancelFn()

	return nil
}

func GetFileAsStream(client proto.FileTransportClient, fileHash string) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(context.Background())

	stream, err := client.GetFile(ctx, &proto.GetReq{
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

type fileWriteCloser struct {
	myio.PromiseWriteCloser[string]
	stream proto.FileTransport_SendFileClient
}

func (s *fileWriteCloser) Write(p []byte) (int, error) {
	err := s.stream.Send(&proto.FileDataPacket{
		Type: proto.FileDataPacketType_Data,
		Data: p,
	})

	if err != nil {
		return 0, err
	}

	return len(p), nil
}

func (s *fileWriteCloser) Abort(err error) {
	s.stream.CloseSend()
}

func (s *fileWriteCloser) Finish() (string, error) {
	err := s.stream.Send(&proto.FileDataPacket{
		Type: proto.FileDataPacketType_EOF,
	})

	if err != nil {
		return "", fmt.Errorf("send EOF packet failed, err: %w", err)
	}

	resp, err := s.stream.CloseAndRecv()
	if err != nil {
		return "", fmt.Errorf("receive response failed, err: %w", err)
	}

	return resp.FileHash, nil
}

func SendFileAsStream(client proto.FileTransportClient) (myio.PromiseWriteCloser[string], error) {
	stream, err := client.SendFile(context.Background())
	if err != nil {
		return nil, err
	}

	return &fileWriteCloser{
		stream: stream,
	}, nil
}
