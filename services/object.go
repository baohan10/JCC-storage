package services

import (
	"fmt"
	"io"
	"sync"

	"gitlink.org.cn/cloudream/client/config"
	"gitlink.org.cn/cloudream/db/model"
	agentcaller "gitlink.org.cn/cloudream/proto"
	racli "gitlink.org.cn/cloudream/rabbitmq/client"
	"gitlink.org.cn/cloudream/utils/consts"
	"gitlink.org.cn/cloudream/utils/consts/errorcode"
	mygrpc "gitlink.org.cn/cloudream/utils/grpc"
	myio "gitlink.org.cn/cloudream/utils/io"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type BucketService = Service

func (svc *BucketService) GetObject(userID int, objectID int) (model.Object, error) {
	// TODO
	panic("not implement yet")
}

func (svc *BucketService) DownloadObject(userID int, objectID int) (io.ReadCloser, error) {

	// 先向协调端请求文件相关的数据
	coorClient, err := racli.NewCoordinatorClient()
	if err != nil {
		return nil, fmt.Errorf("create coordinator client failed, err: %w", err)
	}
	defer coorClient.Close()

	readResp, err := coorClient.Read(objectID, userID)
	if err != nil {
		return nil, fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if readResp.ErrorCode != errorcode.OK {
		return nil, fmt.Errorf("coordinator operation failed, code: %s, message: %s", readResp.ErrorCode, readResp.Message)
	}

	switch readResp.Redundancy {
	case consts.REDUNDANCY_REP:
		if len(readResp.NodeIPs) == 0 {
			return nil, fmt.Errorf("no node has this file")
		}

		// 随便选第一个节点下载文件
		reader, err := svc.downloadAsRepObject(readResp.NodeIPs[0], readResp.Hashes[0])
		if err != nil {
			return nil, fmt.Errorf("rep read failed, err: %w", err)
		}

		return reader, nil

		//case consts.REDUNDANCY_EC:
		// TODO EC部分的代码要考虑重构
		//	ecRead(readResp.FileSizeInBytes, readResp.NodeIPs, readResp.Hashes, readResp.BlockIDs, *readResp.ECName)
	}

	return nil, fmt.Errorf("unsupported redundancy type: %s", readResp.Redundancy)
}

func (svc *BucketService) downloadAsRepObject(nodeIP string, fileHash string) (io.ReadCloser, error) {
	// 连接grpc
	grpcAddr := fmt.Sprintf("%s:%d", nodeIP, config.Cfg().GRPCPort)
	conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
	}
	defer conn.Close()

	/*
		TO DO: 判断本地有没有ipfs daemon、能否获取相应对象的cid
			如果本地有ipfs daemon且能获取相应对象的cid，则获取对象cid对应的ipfsblock的cid，通过ipfs网络获取这些ipfsblock
			否则，像目前一样，使用grpc向指定节点获取
	*/
	// 下载文件
	client := agentcaller.NewFileTransportClient(conn)
	reader, err := mygrpc.GetFileAsStream(client, fileHash)
	if err != nil {
		return nil, fmt.Errorf("request to get file failed, err: %w", err)
	}
	return reader, nil
}

func (svc *BucketService) UploadRepObject(userID int, bucketID int, objectName string, file io.ReadCloser, fileSize int64, repNum int) error {
	coorClient, err := racli.NewCoordinatorClient()
	if err != nil {
		return fmt.Errorf("create coordinator client failed, err: %w", err)
	}
	defer coorClient.Close()

	//发送写请求，请求Coor分配写入节点Ip
	repWriteResp, err := coorClient.RepWrite(bucketID, objectName, fileSize, repNum, userID)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if repWriteResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator RepWrite failed, code: %s, message: %s", repWriteResp.ErrorCode, repWriteResp.Message)
	}

	/*
		TO DO ss: 判断本地有没有ipfs daemon、能否与目标agent的ipfs daemon连通、本地ipfs目录空间是否充足
			如果本地有ipfs daemon、能与目标agent的ipfs daemon连通、本地ipfs目录空间充足，将所有内容写入本地ipfs目录，得到对象的cid，发送cid给目标agent让其pin相应的对象
			否则，像目前一样，使用grpc向指定节点获取
	*/

	senders := make([]fileSender, repNum)

	// 建立grpc连接，发送请求
	svc.startSendFile(repNum, senders, repWriteResp.NodeIDs, repWriteResp.NodeIPs)

	// 向每个节点发送数据
	err = svc.sendFileData(file, repNum, senders)
	if err != nil {
		return err
	}

	// 发送EOF消息，并获得FileHash
	svc.sendFinish(repNum, senders)

	// 收集发送成功的节点以及返回的hash
	var sucNodeIDs []int
	var sucFileHashes []string
	for i := 0; i < repNum; i++ {
		sender := &senders[i]

		if sender.err == nil {
			sucNodeIDs = append(sucNodeIDs, sender.nodeID)
			sucFileHashes = append(sucFileHashes, sender.fileHash)
		}
	}

	// 记录写入的文件的Hash
	// TODO 如果一个都没有写成功，那么是否要发送这个请求？
	writeRepHashResp, err := coorClient.WriteRepHash(bucketID, objectName, fileSize, repNum, userID, sucNodeIDs, sucFileHashes)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if writeRepHashResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator WriteRepHash failed, code: %s, message: %s", writeRepHashResp.ErrorCode, writeRepHashResp.Message)
	}

	return nil
}

type fileSender struct {
	grpcCon  *grpc.ClientConn
	stream   mygrpc.FileWriteCloser[string]
	nodeID   int
	fileHash string
	err      error
}

func (svc *BucketService) startSendFile(numRep int, senders []fileSender, nodeIDs []int, nodeIPs []string) {
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
		stream, err := mygrpc.SendFileAsStream(client)
		if err != nil {
			conn.Close()
			sender.err = fmt.Errorf("request to send file failed, err: %w", err)
			continue
		}

		sender.grpcCon = conn
		sender.stream = stream
	}
}

func (svc *BucketService) sendFileData(file io.ReadCloser, numRep int, senders []fileSender) error {

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

				sender.stream.Abort(io.ErrClosedPipe)
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
				err := myio.WriteAll(sender.stream, buf[:readCnt])

				// 发生错误则关闭连接
				if err != nil {
					sender.stream.Abort(io.ErrClosedPipe)
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

func (svc *BucketService) sendFinish(numRep int, senders []fileSender) {
	for i := 0; i < numRep; i++ {
		sender := &senders[i]

		// 发生了错误的跳过
		if sender.err != nil {
			continue
		}

		fileHash, err := sender.stream.Finish()
		if err != nil {
			sender.err = err
			sender.stream.Abort(io.ErrClosedPipe)
			sender.grpcCon.Close()
			continue
		}

		sender.fileHash = fileHash
		sender.err = err
		sender.grpcCon.Close()
	}
}

func (svc *BucketService) UploadECObject(userID int, file io.ReadCloser, fileSize int64, ecName string) error {
	// TODO
	panic("not implement yet")
}

func (svc *BucketService) MoveObjectToStorage(userID int, objectID int, storageID int) error {
	// 先向协调端请求文件相关的元数据
	coorClient, err := racli.NewCoordinatorClient()
	if err != nil {
		return fmt.Errorf("create coordinator client failed, err: %w", err)
	}
	defer coorClient.Close()

	moveResp, err := coorClient.Move(objectID, storageID, userID)
	if err != nil {
		return fmt.Errorf("request to coordinator failed, err: %w", err)
	}
	if moveResp.ErrorCode != errorcode.OK {
		return fmt.Errorf("coordinator operation failed, code: %s, message: %s", moveResp.ErrorCode, moveResp.Message)
	}

	// 然后向代理端发送移动文件的请求
	agentClient, err := racli.NewAgentClient(moveResp.NodeID)
	if err != nil {
		return fmt.Errorf("create agent client to %d failed, err: %w", storageID, err)
	}
	defer agentClient.Close()

	switch moveResp.Redundancy {
	case consts.REDUNDANCY_REP:
		agentMoveResp, err := agentClient.RepMove(moveResp.Directory, moveResp.Hashes, objectID, userID, moveResp.FileSizeInBytes)
		if err != nil {
			return fmt.Errorf("request to agent %d failed, err: %w", storageID, err)
		}
		if agentMoveResp.ErrorCode != errorcode.OK {
			return fmt.Errorf("agent %d operation failed, code: %s, messsage: %s", storageID, agentMoveResp.ErrorCode, agentMoveResp.Message)
		}

	case consts.REDUNDANCY_EC:
		agentMoveResp, err := agentClient.ECMove(moveResp.Directory, moveResp.Hashes, moveResp.IDs, *moveResp.ECName, objectID, userID, moveResp.FileSizeInBytes)
		if err != nil {
			return fmt.Errorf("request to agent %d failed, err: %w", storageID, err)
		}
		if agentMoveResp.ErrorCode != errorcode.OK {
			return fmt.Errorf("agent %d operation failed, code: %s, messsage: %s", storageID, agentMoveResp.ErrorCode, agentMoveResp.Message)
		}
	}

	return nil
}
