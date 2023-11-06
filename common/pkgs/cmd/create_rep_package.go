package cmd

import (
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type UploadNodeInfo struct {
	Node           model.Node
	IsSameLocation bool
}

type CreateRepPackage struct {
	userID       int64
	bucketID     int64
	name         string
	objectIter   iterator.UploadingObjectIterator
	redundancy   cdssdk.RepRedundancyInfo
	nodeAffinity *int64
}

type UpdatePackageContext struct {
	Distlock *distlock.Service
}

type CreateRepPackageResult struct {
	PackageID     int64
	ObjectResults []RepObjectUploadResult
}

type RepObjectUploadResult struct {
	Info     *iterator.IterUploadingObject
	Error    error
	FileHash string
	ObjectID int64
}

func NewCreateRepPackage(userID int64, bucketID int64, name string, objIter iterator.UploadingObjectIterator, redundancy cdssdk.RepRedundancyInfo, nodeAffinity *int64) *CreateRepPackage {
	return &CreateRepPackage{
		userID:       userID,
		bucketID:     bucketID,
		name:         name,
		objectIter:   objIter,
		redundancy:   redundancy,
		nodeAffinity: nodeAffinity,
	}
}

func (t *CreateRepPackage) Execute(ctx *UpdatePackageContext) (*CreateRepPackageResult, error) {
	defer t.objectIter.Close()

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}

	reqBlder := reqbuilder.NewBuilder()
	// 如果本地的IPFS也是存储系统的一个节点，那么从本地上传时，需要加锁
	if stgglb.Local.NodeID != nil {
		reqBlder.IPFS().CreateAnyRep(*stgglb.Local.NodeID)
	}
	mutex, err := reqBlder.
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

	nodeInfos := lo.Map(getUserNodesResp.Nodes, func(node model.Node, index int) UploadNodeInfo {
		return UploadNodeInfo{
			Node:           node,
			IsSameLocation: node.LocationID == findCliLocResp.Location.LocationID,
		}
	})
	uploadNode := t.chooseUploadNode(nodeInfos, t.nodeAffinity)

	// 防止上传的副本被清除
	ipfsMutex, err := reqbuilder.NewBuilder().
		IPFS().CreateAnyRep(uploadNode.Node.NodeID).
		MutexLock(ctx.Distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer ipfsMutex.Unlock()

	rets, err := uploadAndUpdateRepPackage(createPkgResp.PackageID, t.objectIter, uploadNode)
	if err != nil {
		return nil, err
	}

	return &CreateRepPackageResult{
		PackageID:     createPkgResp.PackageID,
		ObjectResults: rets,
	}, nil
}

func uploadAndUpdateRepPackage(packageID int64, objectIter iterator.UploadingObjectIterator, uploadNode UploadNodeInfo) ([]RepObjectUploadResult, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}

	var uploadRets []RepObjectUploadResult
	var adds []coormq.AddRepObjectInfo
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
			fileHash, err := uploadFile(objInfo.File, uploadNode)
			uploadRets = append(uploadRets, RepObjectUploadResult{
				Info:     objInfo,
				Error:    err,
				FileHash: fileHash,
			})
			if err != nil {
				return fmt.Errorf("uploading object: %w", err)
			}

			adds = append(adds, coormq.NewAddRepObjectInfo(objInfo.Path, objInfo.Size, fileHash, []int64{uploadNode.Node.NodeID}))
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	_, err = coorCli.UpdateRepPackage(coormq.NewUpdateRepPackage(packageID, adds, nil))
	if err != nil {
		return nil, fmt.Errorf("updating package: %w", err)
	}

	return uploadRets, nil
}

// 上传文件
func uploadFile(file io.Reader, uploadNode UploadNodeInfo) (string, error) {
	// 本地有IPFS，则直接从本地IPFS上传
	if stgglb.IPFSPool != nil {
		logger.Infof("try to use local IPFS to upload file")

		// 只有本地IPFS不是存储系统中的一个节点，才需要Pin文件
		fileHash, err := uploadToLocalIPFS(file, uploadNode.Node.NodeID, stgglb.Local.NodeID == nil)
		if err == nil {
			return fileHash, nil

		} else {
			logger.Warnf("upload to local IPFS failed, so try to upload to node %d, err: %s", uploadNode.Node.NodeID, err.Error())
		}
	}

	// 否则发送到agent上传
	// 如果客户端与节点在同一个地域，则使用内网地址连接节点
	nodeIP := uploadNode.Node.ExternalIP
	grpcPort := uploadNode.Node.ExternalGRPCPort
	if uploadNode.IsSameLocation {
		nodeIP = uploadNode.Node.LocalIP
		grpcPort = uploadNode.Node.LocalGRPCPort

		logger.Infof("client and node %d are at the same location, use local ip", uploadNode.Node.NodeID)
	}

	fileHash, err := uploadToNode(file, nodeIP, grpcPort)
	if err != nil {
		return "", fmt.Errorf("upload to node %s failed, err: %w", nodeIP, err)
	}

	return fileHash, nil
}

// chooseUploadNode 选择一个上传文件的节点
// 1. 选择设置了亲和性的节点
// 2. 从与当前客户端相同地域的节点中随机选一个
// 3. 没有用的话从所有节点中随机选一个
func (t *CreateRepPackage) chooseUploadNode(nodes []UploadNodeInfo, nodeAffinity *int64) UploadNodeInfo {
	if nodeAffinity != nil {
		aff, ok := lo.Find(nodes, func(node UploadNodeInfo) bool { return node.Node.NodeID == *nodeAffinity })
		if ok {
			return aff
		}
	}

	sameLocationNodes := lo.Filter(nodes, func(e UploadNodeInfo, i int) bool { return e.IsSameLocation })
	if len(sameLocationNodes) > 0 {
		return sameLocationNodes[rand.Intn(len(sameLocationNodes))]
	}

	return nodes[rand.Intn(len(nodes))]
}

func uploadToNode(file io.Reader, nodeIP string, grpcPort int) (string, error) {
	rpcCli, err := stgglb.AgentRPCPool.Acquire(nodeIP, grpcPort)
	if err != nil {
		return "", fmt.Errorf("new agent rpc client: %w", err)
	}
	defer rpcCli.Close()

	return rpcCli.SendIPFSFile(file)
}

func uploadToLocalIPFS(file io.Reader, nodeID int64, shouldPin bool) (string, error) {
	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return "", fmt.Errorf("new ipfs client: %w", err)
	}
	defer ipfsCli.Close()

	// 从本地IPFS上传文件
	fileHash, err := ipfsCli.CreateFile(file)
	if err != nil {
		return "", fmt.Errorf("creating ipfs file: %w", err)
	}

	if !shouldPin {
		return fileHash, nil
	}

	err = pinIPFSFile(nodeID, fileHash)
	if err != nil {
		return "", err
	}

	return fileHash, nil
}

func pinIPFSFile(nodeID int64, fileHash string) error {
	agtCli, err := stgglb.AgentMQPool.Acquire(nodeID)
	if err != nil {
		return fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	// 然后让最近节点pin本地上传的文件
	pinObjResp, err := agtCli.StartPinningObject(agtmq.NewStartPinningObject(fileHash))
	if err != nil {
		return fmt.Errorf("start pinning object: %w", err)
	}

	for {
		waitResp, err := agtCli.WaitPinningObject(agtmq.NewWaitPinningObject(pinObjResp.TaskID, int64(time.Second)*5))
		if err != nil {
			return fmt.Errorf("waitting pinning object: %w", err)
		}

		if waitResp.IsComplete {
			if waitResp.Error != "" {
				return fmt.Errorf("agent pinning object: %s", waitResp.Error)
			}

			break
		}
	}

	return nil
}
