package iterator

import (
	"fmt"
	"io"
	"math/rand"
	"os"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/distlock/reqbuilder"
	distsvc "gitlink.org.cn/cloudream/common/pkgs/distlock/service"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/storage-common/models"
	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage-common/pkgs/ec"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	myio "gitlink.org.cn/cloudream/common/utils/io"
	agentcaller "gitlink.org.cn/cloudream/storage-common/pkgs/proto"
	mygrpc "gitlink.org.cn/cloudream/storage-common/utils/grpc"
)

type ECObjectIterator struct {
	objects      []model.Object
	objectECData []models.ObjectECData
	currentIndex int
	inited       bool

	coorCli        *coormq.Client
	distlock       *distsvc.Service
	ec             model.Ec
	ecPacketSize   int64
	downloadConfig DownloadConfig
	cliLocation    model.Location
}

func NewECObjectIterator(objects []model.Object, objectECData []models.ObjectECData, coorCli *coormq.Client, distlock *distsvc.Service, ec model.Ec, ecPacketSize int64, downloadConfig DownloadConfig) *ECObjectIterator {
	return &ECObjectIterator{
		objects:        objects,
		objectECData:   objectECData,
		coorCli:        coorCli,
		distlock:       distlock,
		ec:             ec,
		ecPacketSize:   ecPacketSize,
		downloadConfig: downloadConfig,
	}
}

func (i *ECObjectIterator) MoveNext() (*IterDownloadingObject, error) {
	if !i.inited {
		i.inited = true

		findCliLocResp, err := i.coorCli.FindClientLocation(coormq.NewFindClientLocation(i.downloadConfig.ExternalIP))
		if err != nil {
			return nil, fmt.Errorf("finding client location: %w", err)
		}
		i.cliLocation = findCliLocResp.Location
	}

	if i.currentIndex >= len(i.objects) {
		return nil, ErrNoMoreItem
	}

	item, err := i.doMove()
	i.currentIndex++
	return item, err
}

func (iter *ECObjectIterator) doMove() (*IterDownloadingObject, error) {
	obj := iter.objects[iter.currentIndex]
	ecData := iter.objectECData[iter.currentIndex]

	blocks := ecData.Blocks
	ec := iter.ec
	ecK := ec.EcK
	ecN := ec.EcN
	//采取直接读，优先选内网节点
	hashs := make([]string, ecK)
	nds := make([]DownloadNodeInfo, ecK)
	for i := 0; i < ecK; i++ {
		hashs[i] = blocks[i].FileHash

		getNodesResp, err := iter.coorCli.GetNodes(coormq.NewGetNodes(blocks[i].NodeIDs))
		if err != nil {
			return nil, fmt.Errorf("getting nodes: %w", err)
		}

		downloadNodes := lo.Map(getNodesResp.Nodes, func(node model.Node, index int) DownloadNodeInfo {
			return DownloadNodeInfo{
				Node:           node,
				IsSameLocation: node.LocationID == iter.cliLocation.LocationID,
			}
		})

		nds[i] = iter.chooseDownloadNode(downloadNodes)
	}

	//nodeIDs, nodeIPs直接按照第1~ecK个排列
	nodeIDs := make([]int64, ecK)
	nodeIPs := make([]string, ecK)
	for i := 0; i < ecK; i++ {
		nodeIDs[i] = nds[i].Node.NodeID
		nodeIPs[i] = nds[i].Node.ExternalIP
		if nds[i].IsSameLocation {
			nodeIPs[i] = nds[i].Node.LocalIP
			logger.Infof("client and node %d are at the same location, use local ip\n", nds[i].Node.NodeID)
		}
	}

	fileSize := obj.Size
	blockIDs := make([]int, ecK)
	for i := 0; i < ecK; i++ {
		blockIDs[i] = i
	}
	reader, err := iter.downloadEcObject(fileSize, ecK, ecN, blockIDs, nodeIDs, nodeIPs, hashs)
	if err != nil {
		return nil, fmt.Errorf("ec read failed, err: %w", err)
	}

	return &IterDownloadingObject{
		File: reader,
	}, nil
}

func (i *ECObjectIterator) Close() {

}

// chooseDownloadNode 选择一个下载节点
// 1. 从与当前客户端相同地域的节点中随机选一个
// 2. 没有用的话从所有节点中随机选一个
func (i *ECObjectIterator) chooseDownloadNode(entries []DownloadNodeInfo) DownloadNodeInfo {
	sameLocationEntries := lo.Filter(entries, func(e DownloadNodeInfo, i int) bool { return e.IsSameLocation })
	if len(sameLocationEntries) > 0 {
		return sameLocationEntries[rand.Intn(len(sameLocationEntries))]
	}

	return entries[rand.Intn(len(entries))]
}

func (i *ECObjectIterator) downloadObject(nodeID int64, nodeIP string, fileHash string) (io.ReadCloser, error) {
	if i.downloadConfig.LocalIPFS != nil {
		logger.Infof("try to use local IPFS to download file")

		reader, err := i.downloadFromLocalIPFS(fileHash)
		if err == nil {
			return reader, nil
		}

		logger.Warnf("download from local IPFS failed, so try to download from node %s, err: %s", nodeIP, err.Error())
	}

	return i.downloadFromNode(nodeID, nodeIP, fileHash)
}

func (iter *ECObjectIterator) downloadEcObject(fileSize int64, ecK int, ecN int, blockIDs []int, nodeIDs []int64, nodeIPs []string, hashs []string) (io.ReadCloser, error) {
	// TODO zkx 先试用同步方式实现逻辑，做好错误处理。同时也方便下面直接使用uploadToNode和uploadToLocalIPFS来优化代码结构
	//wg := sync.WaitGroup{}
	numPacket := (fileSize + int64(ecK)*iter.ecPacketSize - 1) / (int64(ecK) * iter.ecPacketSize)
	getBufs := make([]chan []byte, ecN)
	decodeBufs := make([]chan []byte, ecK)
	for i := 0; i < ecN; i++ {
		getBufs[i] = make(chan []byte)
	}
	for i := 0; i < ecK; i++ {
		decodeBufs[i] = make(chan []byte)
	}
	for i := 0; i < len(blockIDs); i++ {
		go iter.get(hashs[i], nodeIPs[i], getBufs[blockIDs[i]], numPacket)
	}
	print(numPacket)
	go decode(getBufs[:], decodeBufs[:], blockIDs, ecK, numPacket)
	r, w := io.Pipe()
	//persist函数,将解码得到的文件写入pipe
	go func() {
		for i := 0; int64(i) < numPacket; i++ {
			for j := 0; j < len(decodeBufs); j++ {
				tmp := <-decodeBufs[j]
				_, err := w.Write(tmp)
				if err != nil {
					fmt.Errorf("persist file falied, err:%w", err)
				}
			}
		}
		w.Close()
	}()
	return r, nil
}

func (iter *ECObjectIterator) get(fileHash string, nodeIP string, getBuf chan []byte, numPacket int64) error {
	downloadFromAgent := false
	//使用本地IPFS获取
	if iter.downloadConfig.LocalIPFS != nil {
		logger.Infof("try to use local IPFS to download file")
		//获取IPFS的reader
		reader, err := iter.downloadFromLocalIPFS(fileHash)
		if err != nil {
			downloadFromAgent = true
			fmt.Errorf("read ipfs block failed, err: %w", err)
		}
		defer reader.Close()
		for i := 0; int64(i) < numPacket; i++ {
			buf := make([]byte, iter.ecPacketSize)
			_, err := io.ReadFull(reader, buf)
			if err != nil {
				downloadFromAgent = true
				fmt.Errorf("read file falied, err:%w", err)
			}
			getBuf <- buf
		}
		if downloadFromAgent == false {
			close(getBuf)
			return nil
		}
	} else {
		downloadFromAgent = true
	}
	//从agent获取
	if downloadFromAgent == true {
		/*// 二次获取锁
		mutex, err := reqbuilder.NewBuilder().
			// 用于从IPFS下载文件
			IPFS().ReadOneRep(nodeID, fileHash).
			MutexLock(svc.distlock)
		if err != nil {
			return fmt.Errorf("acquire locks failed, err: %w", err)
		}
		defer mutex.Unlock()
		*/
		// 连接grpc
		grpcAddr := fmt.Sprintf("%s:%d", nodeIP, iter.downloadConfig.GRPCPort)
		conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
		}
		// 下载文件
		client := agentcaller.NewFileTransportClient(conn)
		reader, err := mygrpc.GetFileAsStream(client, fileHash)
		if err != nil {
			conn.Close()
			return fmt.Errorf("request to get file failed, err: %w", err)
		}
		for index := 0; int64(index) < numPacket; index++ {
			buf := make([]byte, iter.ecPacketSize)
			_, _ = reader.Read(buf)
			fmt.Println(buf)
			fmt.Println(numPacket, "\n")
			getBuf <- buf
		}
		close(getBuf)
		reader.Close()
		return nil
	}
	return nil

}

func decode(inBufs []chan []byte, outBufs []chan []byte, blockSeq []int, ecK int, numPacket int64) {
	fmt.Println("decode ")
	var tmpIn [][]byte
	var zeroPkt []byte
	tmpIn = make([][]byte, len(inBufs))
	hasBlock := map[int]bool{}
	for j := 0; j < len(blockSeq); j++ {
		hasBlock[blockSeq[j]] = true
	}
	needRepair := false //检测是否传入了所有数据块
	for j := 0; j < len(outBufs); j++ {
		if blockSeq[j] != j {
			needRepair = true
		}
	}
	enc := ec.NewRsEnc(ecK, len(inBufs))
	for i := 0; int64(i) < numPacket; i++ {
		print("!!!!!")
		for j := 0; j < len(inBufs); j++ {
			if hasBlock[j] {
				tmpIn[j] = <-inBufs[j]
			} else {
				tmpIn[j] = zeroPkt
			}
		}
		if needRepair {
			err := enc.Repair(tmpIn)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Decode Repair Error: %s", err.Error())
			}
		}
		for j := 0; j < len(outBufs); j++ {
			outBufs[j] <- tmpIn[j]
		}
	}
	for i := 0; i < len(outBufs); i++ {
		close(outBufs[i])
	}
}

func (i *ECObjectIterator) downloadFromNode(nodeID int64, nodeIP string, fileHash string) (io.ReadCloser, error) {
	// 二次获取锁
	mutex, err := reqbuilder.NewBuilder().
		// 用于从IPFS下载文件
		IPFS().ReadOneRep(nodeID, fileHash).
		MutexLock(i.distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire locks failed, err: %w", err)
	}

	// 连接grpc
	grpcAddr := fmt.Sprintf("%s:%d", nodeIP, i.downloadConfig.GRPCPort)
	conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
	}

	// 下载文件
	client := agentcaller.NewFileTransportClient(conn)
	reader, err := mygrpc.GetFileAsStream(client, fileHash)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("request to get file failed, err: %w", err)
	}

	reader = myio.AfterReadClosed(reader, func(io.ReadCloser) {
		conn.Close()
		mutex.Unlock()
	})
	return reader, nil
}

func (i *ECObjectIterator) downloadFromLocalIPFS(fileHash string) (io.ReadCloser, error) {
	reader, err := i.downloadConfig.LocalIPFS.OpenRead(fileHash)
	if err != nil {
		return nil, fmt.Errorf("read ipfs file failed, err: %w", err)
	}

	return reader, nil
}
