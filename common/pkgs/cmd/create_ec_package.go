package cmd

import (
	"fmt"
	"io"
	"math/rand"
	"sync"

	"github.com/samber/lo"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	myio "gitlink.org.cn/cloudream/common/utils/io"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type CreateECPackage struct {
	userID       int64
	bucketID     int64
	name         string
	objectIter   iterator.UploadingObjectIterator
	redundancy   cdssdk.ECRedundancyInfo
	nodeAffinity *int64
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

func NewCreateECPackage(userID int64, bucketID int64, name string, objIter iterator.UploadingObjectIterator, redundancy cdssdk.ECRedundancyInfo, nodeAffinity *int64) *CreateECPackage {
	return &CreateECPackage{
		userID:       userID,
		bucketID:     bucketID,
		name:         name,
		objectIter:   objIter,
		redundancy:   redundancy,
		nodeAffinity: nodeAffinity,
	}
}

func (t *CreateECPackage) Execute(ctx *UpdatePackageContext) (*CreateECPackageResult, error) {
	defer t.objectIter.Close()

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
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
		cdssdk.NewTypedRedundancyInfo(t.redundancy)))
	if err != nil {
		return nil, fmt.Errorf("creating package: %w", err)
	}

	getUserNodesResp, err := coorCli.GetUserNodes(coormq.NewGetUserNodes(t.userID))
	if err != nil {
		return nil, fmt.Errorf("getting user nodes: %w", err)
	}

	findCliLocResp, err := coorCli.FindClientLocation(coormq.NewFindClientLocation(stgglb.Local.ExternalIP))
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
	if stgglb.Local.NodeID != nil {
		ipfsReqBlder.IPFS().CreateAnyRep(*stgglb.Local.NodeID)
	}
	for _, node := range uploadNodeInfos {
		if stgglb.Local.NodeID != nil && node.Node.NodeID == *stgglb.Local.NodeID {
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

	// TODO 需要支持设置节点亲和性
	rets, err := uploadAndUpdateECPackage(createPkgResp.PackageID, t.objectIter, uploadNodeInfos, t.redundancy, getECResp.Config)
	if err != nil {
		return nil, err
	}

	return &CreateECPackageResult{
		PackageID:     createPkgResp.PackageID,
		ObjectResults: rets,
	}, nil
}

func uploadAndUpdateECPackage(packageID int64, objectIter iterator.UploadingObjectIterator, uploadNodes []UploadNodeInfo, ecInfo cdssdk.ECRedundancyInfo, ec model.Ec) ([]ECObjectUploadResult, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
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
func uploadECObject(obj *iterator.IterUploadingObject, uploadNodes []UploadNodeInfo, ecInfo cdssdk.ECRedundancyInfo, ecMod model.Ec) ([]string, []int64, error) {
	uploadNodes = shuffleNodes(uploadNodes, ecMod.EcN)

	rs, err := ec.NewRs(ecMod.EcK, ecMod.EcN, ecInfo.ChunkSize)
	if err != nil {
		return nil, nil, err
	}

	outputs := myio.ChunkedSplit(obj.File, ecInfo.ChunkSize, ecMod.EcK, myio.ChunkedSplitOption{
		FillZeros: true,
	})
	var readers []io.Reader
	for _, o := range outputs {
		readers = append(readers, o)
	}
	defer func() {
		for _, o := range outputs {
			o.Close()
		}
	}()

	encStrs := rs.EncodeAll(readers)

	wg := sync.WaitGroup{}

	nodeIDs := make([]int64, ecMod.EcN)
	fileHashes := make([]string, ecMod.EcN)
	anyErrs := make([]error, ecMod.EcN)

	for i := range encStrs {
		idx := i
		wg.Add(1)
		nodeIDs[idx] = uploadNodes[idx].Node.NodeID
		go func() {
			defer wg.Done()
			fileHashes[idx], anyErrs[idx] = uploadFile(encStrs[idx], uploadNodes[idx])
		}()
	}

	wg.Wait()

	for i, e := range anyErrs {
		if e != nil {
			return nil, nil, fmt.Errorf("uploading file to node %d: %w", uploadNodes[i].Node.NodeID, e)
		}
	}

	return fileHashes, nodeIDs, nil
}

func shuffleNodes(uploadNodes []UploadNodeInfo, extendTo int) []UploadNodeInfo {
	for i := len(uploadNodes); i < extendTo; i++ {
		uploadNodes = append(uploadNodes, uploadNodes[rand.Intn(len(uploadNodes))])
	}

	// 随机排列上传节点
	rand.Shuffle(len(uploadNodes), func(i, j int) {
		uploadNodes[i], uploadNodes[j] = uploadNodes[j], uploadNodes[i]
	})

	return uploadNodes
}
