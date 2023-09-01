package cmd

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/models"

	"gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type CreateECPackage struct {
	userID     int64
	bucketID   int64
	name       string
	objectIter iterator.UploadingObjectIterator
	redundancy models.ECRedundancyInfo
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

func NewCreateECPackage(userID int64, bucketID int64, name string, objIter iterator.UploadingObjectIterator, redundancy models.ECRedundancyInfo) *CreateECPackage {
	return &CreateECPackage{
		userID:     userID,
		bucketID:   bucketID,
		name:       name,
		objectIter: objIter,
		redundancy: redundancy,
	}
}

func (t *CreateECPackage) Execute(ctx *UpdatePackageContext) (*CreateECPackageResult, error) {
	defer t.objectIter.Close()

	coorCli, err := globals.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}

	mutex, err := reqbuilder.NewBuilder().
		Metadata().
		// 用于判断用户是否有桶的权限
		UserBucket().ReadOne(t.userID, t.bucketID).
		// 用于查询可用的上传节点
		Node().ReadAny().
		// 用于创建包信息
		Package().CreateOne(t.bucketID, t.name).
		// 用于创建包中的文件的信息
		Object().CreateAny().
		// 用于设置EC配置
		ObjectBlock().CreateAny().
		// 用于创建Cache记录
		Cache().CreateAny().
		MutexLock(ctx.Distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	createPkgResp, err := coorCli.CreatePackage(coormq.NewCreatePackage(t.userID, t.bucketID, t.name,
		models.NewTypedRedundancyInfo(t.redundancy)))
	if err != nil {
		return nil, fmt.Errorf("creating package: %w", err)
	}

	getUserNodesResp, err := coorCli.GetUserNodes(coormq.NewGetUserNodes(t.userID))
	if err != nil {
		return nil, fmt.Errorf("getting user nodes: %w", err)
	}

	findCliLocResp, err := coorCli.FindClientLocation(coormq.NewFindClientLocation(globals.Local.ExternalIP))
	if err != nil {
		return nil, fmt.Errorf("finding client location: %w", err)
	}

	uploadNodeInfos := lo.Map(getUserNodesResp.Nodes, func(node model.Node, index int) UploadNodeInfo {
		return UploadNodeInfo{
			Node:           node,
			IsSameLocation: node.LocationID == findCliLocResp.Location.LocationID,
		}
	})

	getECResp, err := coorCli.GetECConfig(coormq.NewGetECConfig(t.redundancy.ECName))
	if err != nil {
		return nil, fmt.Errorf("getting ec: %w", err)
	}

	// 给上传节点的IPFS加锁
	ipfsReqBlder := reqbuilder.NewBuilder()
	// 如果本地的IPFS也是存储系统的一个节点，那么从本地上传时，需要加锁
	if globals.Local.NodeID != nil {
		ipfsReqBlder.IPFS().CreateAnyRep(*globals.Local.NodeID)
	}
	for _, node := range uploadNodeInfos {
		if globals.Local.NodeID != nil && node.Node.NodeID == *globals.Local.NodeID {
			continue
		}

		ipfsReqBlder.IPFS().CreateAnyRep(node.Node.NodeID)
	}
	// 防止上传的副本被清除
	ipfsMutex, err := ipfsReqBlder.MutexLock(ctx.Distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer ipfsMutex.Unlock()

	rets, err := uploadAndUpdateECPackage(createPkgResp.PackageID, t.objectIter, uploadNodeInfos, t.redundancy, getECResp.Config)
	if err != nil {
		return nil, err
	}

	return &CreateECPackageResult{
		PackageID:     createPkgResp.PackageID,
		ObjectResults: rets,
	}, nil
}

func uploadAndUpdateECPackage(packageID int64, objectIter iterator.UploadingObjectIterator, uploadNodes []UploadNodeInfo, ecInfo models.ECRedundancyInfo, ec model.Ec) ([]ECObjectUploadResult, error) {
	coorCli, err := globals.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}

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
		err = func() error {
			defer objInfo.File.Close()

			fileHashes, uploadedNodeIDs, err := uploadECObject(objInfo, uploadNodes, ecInfo, ec)
			uploadRets = append(uploadRets, ECObjectUploadResult{
				Info:  objInfo,
				Error: err,
			})
			if err != nil {
				return fmt.Errorf("uploading object: %w", err)
			}

			adds = append(adds, coormq.NewAddECObjectInfo(objInfo.Path, objInfo.Size, fileHashes, uploadedNodeIDs))
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	_, err = coorCli.UpdateECPackage(coormq.NewUpdateECPackage(packageID, adds, nil))
	if err != nil {
		return nil, fmt.Errorf("updating package: %w", err)
	}

	return uploadRets, nil
}

// 上传文件
func uploadECObject(obj *iterator.IterUploadingObject, uploadNodes []UploadNodeInfo, ecInfo models.ECRedundancyInfo, ec model.Ec) ([]string, []int64, error) {
	//生成纠删码的写入节点序列
	nodes := make([]UploadNodeInfo, ec.EcN)
	numNodes := len(uploadNodes)
	startWriteNodeID := rand.Intn(numNodes)
	for i := 0; i < ec.EcN; i++ {
		nodes[i] = uploadNodes[(startWriteNodeID+i)%numNodes]
	}

	hashs, err := ecWrite(obj.File, obj.Size, ecInfo.PacketSize, ec.EcK, ec.EcN, nodes)
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

func ecWrite(file io.ReadCloser, fileSize int64, packetSize int64, ecK int, ecN int, nodes []UploadNodeInfo) ([]string, error) {
	// TODO 需要参考RepWrite函数的代码逻辑，做好错误处理
	//获取文件大小

	var coefs = [][]int64{{1, 1, 1}, {1, 2, 3}} //2应替换为ecK，3应替换为ecN
	//计算每个块的packet数
	numPacket := (fileSize + int64(ecK)*packetSize - 1) / (int64(ecK) * packetSize)
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
	go load(file, loadBufs[:ecN], ecK, numPacket*int64(ecK), packetSize) //从本地文件系统加载数据
	go encode(loadBufs[:ecN], encodeBufs[:ecN], ecK, coefs, numPacket)

	var wg sync.WaitGroup
	wg.Add(ecN)

	for idx := 0; idx < ecN; idx++ {
		i := idx
		reader := channelBytesReader{
			channel:     encodeBufs[idx],
			packetCount: numPacket,
		}
		go func() {
			// TODO 处理错误
			fileHash, _ := uploadFile(&reader, nodes[i])
			hashs[i] = fileHash
			wg.Done()
		}()
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

type channelBytesReader struct {
	channel     chan []byte
	packetCount int64
	readingData []byte
}

func (r *channelBytesReader) Read(buf []byte) (int, error) {
	if len(r.readingData) == 0 {
		if r.packetCount == 0 {
			return 0, io.EOF
		}

		r.readingData = <-r.channel
		r.packetCount--
	}

	len := copy(buf, r.readingData)
	r.readingData = r.readingData[:len]

	return len, nil
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
