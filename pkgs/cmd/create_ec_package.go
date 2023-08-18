package cmd

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/models"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	mygrpc "gitlink.org.cn/cloudream/storage-common/utils/grpc"

	"gitlink.org.cn/cloudream/storage-common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage-common/pkgs/ec"
	"gitlink.org.cn/cloudream/storage-common/pkgs/iterator"
	agtmq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage-common/pkgs/mq/coordinator"
	agentcaller "gitlink.org.cn/cloudream/storage-common/pkgs/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type CreateECPackage struct {
	userID       int64
	bucketID     int64
	name         string
	objectIter   iterator.UploadingObjectIterator
	redundancy   models.ECRedundancyInfo
	ecPacketSize int64
	uploadConfig UploadConfig

	Result CreateECPackageResult
}

type CreateECPackageResult struct {
	PackageID     int64
	ObjectResults []ECObjectUploadResult
}

type ECObjectUploadResult struct {
	Info     *iterator.IterUploadingObject
	Error    error
	ObjectID int64
}

func NewCreateECPackage(userID int64, bucketID int64, name string, objIter iterator.UploadingObjectIterator, redundancy models.ECRedundancyInfo, ecPacketSize int64, uploadConfig UploadConfig) *CreateECPackage {
	return &CreateECPackage{
		userID:       userID,
		bucketID:     bucketID,
		name:         name,
		objectIter:   objIter,
		redundancy:   redundancy,
		ecPacketSize: ecPacketSize,
		uploadConfig: uploadConfig,
	}
}

func (t *CreateECPackage) Execute(ctx TaskContext, complete CompleteFn) {
	err := t.do(ctx)
	t.objectIter.Close()
	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *CreateECPackage) do(ctx TaskContext) error {
	// TODO2
	/*
		reqBlder := reqbuilder.NewBuilder()
		for _, uploadObject := range t.Objects {
			reqBlder.Metadata().
				// 用于防止创建了多个同名对象
				Object().CreateOne(t.bucketID, uploadObject.ObjectName)
		}

		// 如果本地的IPFS也是存储系统的一个节点，那么从本地上传时，需要加锁
		if t.uploadConfig.LocalNodeID != nil {
			reqBlder.IPFS().CreateAnyRep(*t.uploadConfig.LocalNodeID)
		}

		mutex, err := reqBlder.
			Metadata().
			// 用于判断用户是否有桶的权限
			UserBucket().ReadOne(t.userID, t.bucketID).
			// 用于查询可用的上传节点
			Node().ReadAny().
			// 用于设置Rep配置
			ObjectRep().CreateAny().
			// 用于创建Cache记录
			Cache().CreateAny().
			MutexLock(ctx.DistLock())
		if err != nil {
			return fmt.Errorf("acquire locks failed, err: %w", err)
		}
		defer mutex.Unlock()
	*/

	createPkgResp, err := ctx.Coordinator().CreatePackage(coormq.NewCreatePackage(t.userID, t.bucketID, t.name,
		models.NewTypedRedundancyInfo(models.RedundancyRep, t.redundancy)))
	if err != nil {
		return fmt.Errorf("creating package: %w", err)
	}

	getUserNodesResp, err := ctx.Coordinator().GetUserNodes(coormq.NewGetUserNodes(t.userID))
	if err != nil {
		return fmt.Errorf("getting user nodes: %w", err)
	}

	findCliLocResp, err := ctx.Coordinator().FindClientLocation(coormq.NewFindClientLocation(t.uploadConfig.ExternalIP))
	if err != nil {
		return fmt.Errorf("finding client location: %w", err)
	}

	uploadNodeInfos := lo.Map(getUserNodesResp.Nodes, func(node model.Node, index int) UploadNodeInfo {
		return UploadNodeInfo{
			Node:           node,
			IsSameLocation: node.LocationID == findCliLocResp.Location.LocationID,
		}
	})

	getECResp, err := ctx.Coordinator().GetECConfig(coormq.NewGetECConfig(t.redundancy.ECName))
	if err != nil {
		return fmt.Errorf("getting ec: %w", err)
	}

	/*
		TODO2
		// 防止上传的副本被清除
		mutex2, err := reqbuilder.NewBuilder().
			IPFS().CreateAnyRep(uploadNode.Node.NodeID).
			MutexLock(ctx.DistLock())
		if err != nil {
			return fmt.Errorf("acquire locks failed, err: %w", err)
		}
		defer mutex2.Unlock()
	*/

	rets, err := uploadAndUpdateECPackage(ctx, createPkgResp.PackageID, t.objectIter, uploadNodeInfos, getECResp.Config, t.ecPacketSize, t.uploadConfig)
	if err != nil {
		return err
	}

	t.Result.PackageID = createPkgResp.PackageID
	t.Result.ObjectResults = rets
	return nil
}

func uploadAndUpdateECPackage(ctx TaskContext, packageID int64, objectIter iterator.UploadingObjectIterator, uploadNodes []UploadNodeInfo, ec model.Ec, ecPacketSize int64, uploadConfig UploadConfig) ([]ECObjectUploadResult, error) {
	var uploadRets []ECObjectUploadResult
	//上传文件夹
	var adds []coormq.AddECObjectInfo
	for {
		objInfo, err := objectIter.MoveNext()
		if err == iterator.ErrNoMoreItem {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading object: %w", err)
		}

		fileHashes, uploadedNodeIDs, err := uploadECObject(ctx, objInfo, uploadNodes, ec, ecPacketSize, uploadConfig)
		uploadRets = append(uploadRets, ECObjectUploadResult{
			Info:  objInfo,
			Error: err,
		})
		if err != nil {
			return nil, fmt.Errorf("uploading object: %w", err)
		}

		adds = append(adds, coormq.NewAddECObjectInfo(objInfo.Path, objInfo.Size, fileHashes, uploadedNodeIDs))
	}

	_, err := ctx.Coordinator().UpdateECPackage(coormq.NewUpdateECPackage(packageID, adds, nil))
	if err != nil {
		return nil, fmt.Errorf("updating package: %w", err)
	}

	return uploadRets, nil
}

// 上传文件
func uploadECObject(ctx TaskContext, obj *iterator.IterUploadingObject, uploadNodes []UploadNodeInfo, ec model.Ec, ecPacketSize int64, uploadConfig UploadConfig) ([]string, []int64, error) {
	//生成纠删码的写入节点序列
	nodes := make([]UploadNodeInfo, ec.EcN)
	numNodes := len(uploadNodes)
	startWriteNodeID := rand.Intn(numNodes)
	for i := 0; i < ec.EcN; i++ {
		nodes[i] = uploadNodes[(startWriteNodeID+i)%numNodes]
	}

	hashs, err := ecWrite(obj.File, obj.Size, ec.EcK, ec.EcN, nodes, ecPacketSize, uploadConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("EcWrite failed, err: %w", err)
	}

	nodeIDs := make([]int64, len(nodes))
	for i := 0; i < len(nodes); i++ {
		nodeIDs[i] = nodes[i].Node.NodeID
	}

	return hashs, nodeIDs, nil
}

// chooseUploadNode 选择一个上传文件的节点
// 1. 从与当前客户端相同地域的节点中随机选一个
// 2. 没有用的话从所有节点中随机选一个
func (t *CreateECPackage) chooseUploadNode(nodes []UploadNodeInfo) UploadNodeInfo {
	sameLocationNodes := lo.Filter(nodes, func(e UploadNodeInfo, i int) bool { return e.IsSameLocation })
	if len(sameLocationNodes) > 0 {
		return sameLocationNodes[rand.Intn(len(sameLocationNodes))]
	}

	return nodes[rand.Intn(len(nodes))]
}

func ecWrite(file io.ReadCloser, fileSize int64, ecK int, ecN int, nodes []UploadNodeInfo, ecPacketSize int64, uploadConfig UploadConfig) ([]string, error) {

	// TODO 需要参考RepWrite函数的代码逻辑，做好错误处理
	//获取文件大小

	var coefs = [][]int64{{1, 1, 1}, {1, 2, 3}} //2应替换为ecK，3应替换为ecN
	//计算每个块的packet数
	numPacket := (fileSize + int64(ecK)*ecPacketSize - 1) / (int64(ecK) * ecPacketSize)
	//fmt.Println(numPacket)
	//创建channel
	loadBufs := make([]chan []byte, ecN)
	encodeBufs := make([]chan []byte, ecN)
	for i := 0; i < ecN; i++ {
		loadBufs[i] = make(chan []byte)
	}
	for i := 0; i < ecN; i++ {
		encodeBufs[i] = make(chan []byte)
	}
	hashs := make([]string, ecN)
	//正式开始写入
	go load(file, loadBufs[:ecN], ecK, numPacket*int64(ecK), ecPacketSize) //从本地文件系统加载数据
	go encode(loadBufs[:ecN], encodeBufs[:ecN], ecK, coefs, numPacket)

	var wg sync.WaitGroup
	wg.Add(ecN)
	/*mutex, err := reqbuilder.NewBuilder().
		// 防止上传的副本被清除
		IPFS().CreateAnyRep(node.ID).
		MutexLock(svc.distlock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()
	*/
	for i := 0; i < ecN; i++ {
		go send(nodes[i], encodeBufs[i], numPacket, &wg, hashs, i, uploadConfig)
	}
	wg.Wait()

	return hashs, nil

}

func load(file io.ReadCloser, loadBufs []chan []byte, ecK int, totalNumPacket int64, ecPacketSize int64) error {

	for i := 0; int64(i) < totalNumPacket; i++ {

		buf := make([]byte, ecPacketSize)
		idx := i % ecK
		_, err := file.Read(buf)
		if err != nil {
			return fmt.Errorf("read file falied, err:%w", err)
		}
		loadBufs[idx] <- buf

		if idx == ecK-1 {
			for j := ecK; j < len(loadBufs); j++ {
				zeroPkt := make([]byte, ecPacketSize)
				loadBufs[j] <- zeroPkt
			}
		}
		if err != nil && err != io.EOF {
			return fmt.Errorf("load file to buf failed, err:%w", err)
		}
	}
	for i := 0; i < len(loadBufs); i++ {

		close(loadBufs[i])
	}
	file.Close()
	return nil
}

func encode(inBufs []chan []byte, outBufs []chan []byte, ecK int, coefs [][]int64, numPacket int64) {
	var tmpIn [][]byte
	tmpIn = make([][]byte, len(outBufs))
	enc := ec.NewRsEnc(ecK, len(outBufs))
	for i := 0; int64(i) < numPacket; i++ {
		for j := 0; j < len(outBufs); j++ {
			tmpIn[j] = <-inBufs[j]
		}
		enc.Encode(tmpIn)
		for j := 0; j < len(outBufs); j++ {
			outBufs[j] <- tmpIn[j]
		}
	}
	for i := 0; i < len(outBufs); i++ {
		close(outBufs[i])
	}
}

func send(node UploadNodeInfo, inBuf chan []byte, numPacket int64, wg *sync.WaitGroup, hashs []string, idx int, uploadConfig UploadConfig) error {
	// TODO zkx 先直接复制client\internal\task\upload_rep_objects.go中的uploadToNode和uploadToLocalIPFS来替代这部分逻辑
	// 方便之后异步化处理
	// uploadToAgent的逻辑反了，而且中间步骤失败，就必须打印日志后停止后续操作

	uploadToAgent := true
	if uploadConfig.LocalIPFS != nil { //使用IPFS传输
		//创建IPFS文件
		logger.Infof("try to use local IPFS to upload block")
		writer, err := uploadConfig.LocalIPFS.CreateFile()
		if err != nil {
			uploadToAgent = false
			fmt.Errorf("create IPFS file failed, err: %w", err)
		}
		//逐packet写进ipfs
		for i := 0; int64(i) < numPacket; i++ {
			buf := <-inBuf
			reader := bytes.NewReader(buf)
			_, err = io.Copy(writer, reader)
			if err != nil {
				uploadToAgent = false
				fmt.Errorf("copying block data to IPFS file failed, err: %w", err)
			}
		}
		//finish, 获取哈希
		fileHash, err := writer.Finish()
		if err != nil {
			logger.Warnf("upload block to local IPFS failed, so try to upload by agent, err: %s", err.Error())
			uploadToAgent = false
			fmt.Errorf("finish writing blcok to IPFS failed, err: %w", err)
		}
		hashs[idx] = fileHash
		if err != nil {
		}
		nodeID := node.Node.NodeID
		// 然后让最近节点pin本地上传的文件
		agentClient, err := agtmq.NewClient(nodeID, uploadConfig.MQ)
		if err != nil {
			uploadToAgent = false
			fmt.Errorf("create agent client to %d failed, err: %w", nodeID, err)
		}
		defer agentClient.Close()

		pinObjResp, err := agentClient.StartPinningObject(agtmq.NewStartPinningObject(fileHash))
		if err != nil {
			uploadToAgent = false
			fmt.Errorf("start pinning object: %w", err)
		}
		for {
			waitResp, err := agentClient.WaitPinningObject(agtmq.NewWaitPinningObject(pinObjResp.TaskID, int64(time.Second)*5))
			if err != nil {
				uploadToAgent = false
				fmt.Errorf("waitting pinning object: %w", err)
			}
			if waitResp.IsComplete {
				if waitResp.Error != "" {
					uploadToAgent = false
					fmt.Errorf("agent pinning object: %s", waitResp.Error)
				}
				break
			}
		}
		if uploadToAgent == false {
			return nil
		}
	}
	//////////////////////////////通过Agent上传
	if uploadToAgent == true {
		// 如果客户端与节点在同一个地域，则使用内网地址连接节点
		nodeIP := node.Node.ExternalIP
		if node.IsSameLocation {
			nodeIP = node.Node.LocalIP

			logger.Infof("client and node %d are at the same location, use local ip\n", node.Node.NodeID)
		}

		grpcAddr := fmt.Sprintf("%s:%d", nodeIP, uploadConfig.GRPCPort)
		grpcCon, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("connect to grpc server at %s failed, err: %w", grpcAddr, err)
		}
		defer grpcCon.Close()

		client := agentcaller.NewFileTransportClient(grpcCon)
		upload, err := mygrpc.SendFileAsStream(client)
		if err != nil {
			return fmt.Errorf("request to send file failed, err: %w", err)
		}
		// 发送文件数据
		for i := 0; int64(i) < numPacket; i++ {
			buf := <-inBuf
			reader := bytes.NewReader(buf)
			_, err = io.Copy(upload, reader)
			if err != nil {
				// 发生错误则关闭连接
				upload.Abort(io.ErrClosedPipe)
				return fmt.Errorf("copy block date to upload stream failed, err: %w", err)
			}
		}
		// 发送EOF消息，并获得FileHash
		fileHash, err := upload.Finish()
		if err != nil {
			upload.Abort(io.ErrClosedPipe)
			return fmt.Errorf("send EOF failed, err: %w", err)
		}
		hashs[idx] = fileHash
		wg.Done()
	}
	return nil
}

func persist(inBuf []chan []byte, numPacket int64, localFilePath string, wg *sync.WaitGroup) {
	fDir, err := os.Executable()
	if err != nil {
		panic(err)
	}
	fURL := filepath.Join(filepath.Dir(fDir), "assets")
	_, err = os.Stat(fURL)
	if os.IsNotExist(err) {
		os.MkdirAll(fURL, os.ModePerm)
	}
	file, err := os.Create(filepath.Join(fURL, localFilePath))
	if err != nil {
		return
	}
	for i := 0; int64(i) < numPacket; i++ {
		for j := 0; j < len(inBuf); j++ {
			tmp := <-inBuf[j]
			fmt.Println(tmp)
			file.Write(tmp)
		}
	}
	file.Close()
	wg.Done()
}
