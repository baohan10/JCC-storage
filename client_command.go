package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"gitlink.org.cn/cloudream/client/config"
	agentcaller "gitlink.org.cn/cloudream/proto"

	racli "gitlink.org.cn/cloudream/rabbitmq/client"
	"gitlink.org.cn/cloudream/utils/consts"
	"gitlink.org.cn/cloudream/utils/consts/errorcode"
	myio "gitlink.org.cn/cloudream/utils/io"

	"google.golang.org/grpc"

	_ "google.golang.org/grpc/balancer/grpclb"
	"google.golang.org/grpc/credentials/insecure"
)

func Move(bucketName string, objectName string, stgID int) error {
	// TODO 此处是写死的常量
	userId := 0

	// 先向协调端请求文件相关的元数据
	coorClient, err := racli.NewCoordinatorClient()
	if err != nil {
		return fmt.Errorf("create coordinator client failed, err: %w", err)
	}
	defer coorClient.Close()

	moveResp, err := coorClient.Move(bucketName, objectName, userId, stgID)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if moveResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator operation failed, code: %s, message: %s", moveResp.ErrorCode, moveResp.Message)
	}

	// 然后向代理端发送移动文件的请求
	agentClient, err := racli.NewAgentClient(moveResp.NodeID)
	if err != nil {
		return fmt.Errorf("create agent client to %d failed, err: %w", stgID, err)
	}
	defer agentClient.Close()

	switch moveResp.Redundancy {
	case consts.REDUNDANCY_REP:
		agentMoveResp, err := agentClient.RepMove(moveResp.Directory, moveResp.Hashes, bucketName, objectName, userId, moveResp.FileSizeInBytes)
		if err != nil {
			return fmt.Errorf("request to agent %d failed, err: %w", stgID, err)
		}
		if agentMoveResp.ErrorCode != errorcode.OK {
			return fmt.Errorf("agent %d operation failed, code: %s, messsage: %s", stgID, agentMoveResp.ErrorCode, agentMoveResp.Message)
		}

	case consts.REDUNDANCY_EC:
		agentMoveResp, err := agentClient.ECMove(moveResp.Directory, moveResp.Hashes, moveResp.IDs, moveResp.ECName, bucketName, objectName, userId, moveResp.FileSizeInBytes)
		if err != nil {
			return fmt.Errorf("request to agent %d failed, err: %w", stgID, err)
		}
		if agentMoveResp.ErrorCode != errorcode.OK {
			return fmt.Errorf("agent %d operation failed, code: %s, messsage: %s", stgID, agentMoveResp.ErrorCode, agentMoveResp.Message)
		}
	}

	return nil
}

func Read(localFilePath string, bucketName string, objectName string) error {
	// TODO 此处是写死的常量
	userId := 0

	// 先向协调端请求文件相关的数据
	coorClient, err := racli.NewCoordinatorClient()
	if err != nil {
		return fmt.Errorf("create coordinator client failed, err: %w", err)
	}
	defer coorClient.Close()

	readResp, err := coorClient.Read(bucketName, objectName, userId)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if readResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator operation failed, code: %s, message: %s", readResp.ErrorCode, readResp.Message)
	}

	switch readResp.Redundancy {
	case consts.REDUNDANCY_REP:
		if len(readResp.NodeIPs) == 0 {
			return fmt.Errorf("no node has this file")
		}

		// 随便选第一个节点下载文件
		err = repRead(readResp.FileSizeInBytes, readResp.NodeIPs[0], readResp.Hashes[0], localFilePath)
		if err != nil {
			return fmt.Errorf("rep read failed, err: %w", err)
		}

	case consts.REDUNDANCY_EC:
		// TODO EC部分的代码要考虑重构
		//ecRead(readResp.FileSizeInBytes, readResp.NodeIPs, readResp.Hashes, readResp.BlockIDs, readResp.ECName, localFilePath)
	}

	return nil
}

func repRead(fileSizeInBytes int64, nodeIP string, repHash string, localFilePath string) error {
	// 连接grpc
	grpcAddr := fmt.Sprintf("%s:%d", nodeIP, config.Cfg().GRPCPort)
	conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
	}
	defer conn.Close()

	// 创建本地文件
	curExecPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("get executable directory failed, err: %w", err)
	}

	outputFilePath := filepath.Join(filepath.Dir(curExecPath), localFilePath)
	outputFileDir := filepath.Dir(outputFilePath)

	err = os.MkdirAll(outputFileDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("create output file directory %s failed, err: %w", outputFileDir, err)
	}

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return fmt.Errorf("create output file %s failed, err: %w", outputFilePath, err)
	}
	defer outputFile.Close()

	/*
		TO DO: 判断本地有没有ipfs daemon、能否获取相应对象的cid
			如果本地有ipfs daemon且能获取相应对象的cid，则获取对象cid对应的ipfsblock的cid，通过ipfs网络获取这些ipfsblock
			否则，像目前一样，使用grpc向指定节点获取
	*/
	// 下载文件
	client := agentcaller.NewFileTransportClient(conn)
	stream, err := client.GetFile(context.Background(), &agentcaller.GetReq{
		FileHash: repHash,
	})
	if err != nil {
		return fmt.Errorf("request grpc failed, err: %w", err)
	}
	defer stream.CloseSend()

	for {
		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("read file data on grpc stream failed, err: %w", err)
		}

		if resp.Type == agentcaller.FileDataPacketType_Data {
			err = myio.WriteAll(outputFile, resp.Data)
			// TODO 写入到文件失败，是否要考虑删除这个不完整的文件？
			if err != nil {
				return fmt.Errorf("write file data to local file failed, err: %w", err)
			}

		} else if resp.Type == agentcaller.FileDataPacketType_EOF {
			return nil
		}
	}
}

type fileSender struct {
	grpcCon  *grpc.ClientConn
	stream   agentcaller.FileTransport_SendFileClient
	nodeID   int
	fileHash string
	err      error
}

func RepWrite(localFilePath string, bucketName string, objectName string, numRep int) error {
	// TODO 此处是写死的常量
	userId := 0

	//获取文件大小
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		return fmt.Errorf("get file %s state failed, err: %w", localFilePath, err)
	}
	fileSizeInBytes := fileInfo.Size()

	coorClient, err := racli.NewCoordinatorClient()
	if err != nil {
		return fmt.Errorf("create coordinator client failed, err: %w", err)
	}
	defer coorClient.Close()

	//发送写请求，请求Coor分配写入节点Ip
	repWriteResp, err := coorClient.RepWrite(bucketName, objectName, fileSizeInBytes, numRep, userId)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if repWriteResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator RepWrite failed, err: %w", err)
	}

	file, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("open file %s failed, err: %w", localFilePath, err)
	}
	defer file.Close()

	/*
		TO DO ss: 判断本地有没有ipfs daemon、能否与目标agent的ipfs daemon连通、本地ipfs目录空间是否充足
			如果本地有ipfs daemon、能与目标agent的ipfs daemon连通、本地ipfs目录空间充足，将所有内容写入本地ipfs目录，得到对象的cid，发送cid给目标agent让其pin相应的对象
			否则，像目前一样，使用grpc向指定节点获取
	*/

	senders := make([]fileSender, numRep)

	// 建立grpc连接，发送请求
	startSendFile(numRep, senders, repWriteResp.NodeIDs, repWriteResp.NodeIPs)

	// 向每个节点发送数据
	err = sendFileData(file, numRep, senders)
	if err != nil {
		return err
	}

	// 发送EOF消息，并获得FileHash
	sendFinish(numRep, senders)

	// 收集发送成功的节点以及返回的hash
	var sucNodeIDs []int
	var sucFileHashes []string
	for i := 0; i < numRep; i++ {
		sender := &senders[i]

		if sender.err == nil {
			sucNodeIDs = append(sucNodeIDs, sender.nodeID)
			sucFileHashes = append(sucFileHashes, sender.fileHash)
		}
	}

	// 记录写入的文件的Hash
	// TODO 如果一个都没有写成功，那么是否要发送这个请求？
	writeRepHashResp, err := coorClient.WriteRepHash(bucketName, objectName, fileSizeInBytes, numRep, userId, sucNodeIDs, sucFileHashes)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if writeRepHashResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator WriteRepHash failed, err: %w", err)
	}

	return nil
}

func startSendFile(numRep int, senders []fileSender, nodeIDs []int, nodeIPs []string) {
	for i := 0; i < numRep; i++ {
		sender := &senders[i]

		sender.nodeID = nodeIDs[i]

		grpcAddr := fmt.Sprintf("%s:%d", nodeIPs[i], config.Cfg().GRPCPort)
		conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

		if err != nil {
			sender.err = fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
			continue
		}

		client := agentcaller.NewFileTransportClient(conn)
		stream, err := client.SendFile(context.Background())
		if err != nil {
			conn.Close()
			sender.err = fmt.Errorf("request to send file failed, err: %w", err)
			continue
		}

		sender.grpcCon = conn
		sender.stream = stream
	}
}

func sendFileData(file *os.File, numRep int, senders []fileSender) error {

	// 共用的发送数据缓冲区
	buf := make([]byte, 2048)

	for {
		// 读取文件数据
		readCnt, err := file.Read(buf)

		// 文件读取完毕
		if err == io.EOF {
			break
		}

		if err != nil {
			// 读取失败则断开所有连接
			for i := 0; i < numRep; i++ {
				sender := &senders[i]

				if sender.err != nil {
					continue
				}

				sender.stream.CloseSend()
				sender.grpcCon.Close()
				sender.err = fmt.Errorf("read file data failed, err: %w", err)
			}

			return fmt.Errorf("read file data failed, err: %w", err)
		}

		// 并行的向每个节点发送数据
		hasSender := false
		var sendWg sync.WaitGroup
		for i := 0; i < numRep; i++ {
			sender := &senders[i]

			// 发生了错误的跳过
			if sender.err != nil {
				continue
			}

			hasSender = true

			sendWg.Add(1)
			go func() {
				err := sender.stream.Send(&agentcaller.FileDataPacket{
					Type: agentcaller.FileDataPacketType_Data,
					Data: buf[:readCnt],
				})

				// 发生错误则关闭连接
				if err != nil {
					sender.stream.CloseSend()
					sender.grpcCon.Close()
					sender.err = fmt.Errorf("send file data failed, err: %w", err)
				}

				sendWg.Done()
			}()
		}

		// 等待向每个节点发送数据结束
		sendWg.Wait()

		// 如果所有节点都发送失败，则不要再继续读取文件数据了
		if !hasSender {
			break
		}
	}
	return nil
}

func sendFinish(numRep int, senders []fileSender) {
	for i := 0; i < numRep; i++ {
		sender := &senders[i]

		// 发生了错误的跳过
		if sender.err != nil {
			continue
		}

		err := sender.stream.Send(&agentcaller.FileDataPacket{
			Type: agentcaller.FileDataPacketType_EOF,
		})
		if err != nil {
			sender.stream.CloseSend()
			sender.grpcCon.Close()
			sender.err = fmt.Errorf("send file data failed, err: %w", err)
			continue
		}

		resp, err := sender.stream.CloseAndRecv()
		if err != nil {
			sender.err = fmt.Errorf("receive response failed, err: %w", err)
			sender.grpcCon.Close()
			continue
		}

		sender.fileHash = resp.FileHash
		sender.grpcCon.Close()
	}
}
