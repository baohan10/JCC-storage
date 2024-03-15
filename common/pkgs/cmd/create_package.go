package cmd

import (
	"fmt"
	"io"
	"math/rand"

	"github.com/samber/lo"

	"gitlink.org.cn/cloudream/common/pkgs/distlock"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/iterator"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type CreatePackage struct {
	userID       cdssdk.UserID
	bucketID     cdssdk.BucketID
	name         string
	objectIter   iterator.UploadingObjectIterator
	nodeAffinity *cdssdk.NodeID
}

type CreatePackageResult struct {
	PackageID     cdssdk.PackageID
	ObjectResults []ObjectUploadResult
}

type ObjectUploadResult struct {
	Info  *iterator.IterUploadingObject
	Error error
	// TODO 这个字段没有被赋值
	ObjectID cdssdk.ObjectID
}

type UploadNodeInfo struct {
	Node           cdssdk.Node
	IsSameLocation bool
}

type UpdatePackageContext struct {
	Distlock *distlock.Service
}

func NewCreatePackage(userID cdssdk.UserID, bucketID cdssdk.BucketID, name string, objIter iterator.UploadingObjectIterator, nodeAffinity *cdssdk.NodeID) *CreatePackage {
	return &CreatePackage{
		userID:       userID,
		bucketID:     bucketID,
		name:         name,
		objectIter:   objIter,
		nodeAffinity: nodeAffinity,
	}
}

func (t *CreatePackage) Execute(ctx *UpdatePackageContext) (*CreatePackageResult, error) {
	defer t.objectIter.Close()

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}

	createPkgResp, err := coorCli.CreatePackage(coormq.NewCreatePackage(t.userID, t.bucketID, t.name))
	if err != nil {
		return nil, fmt.Errorf("creating package: %w", err)
	}

	getUserNodesResp, err := coorCli.GetUserNodes(coormq.NewGetUserNodes(t.userID))
	if err != nil {
		return nil, fmt.Errorf("getting user nodes: %w", err)
	}

	userNodes := lo.Map(getUserNodesResp.Nodes, func(node cdssdk.Node, index int) UploadNodeInfo {
		return UploadNodeInfo{
			Node:           node,
			IsSameLocation: node.LocationID == stgglb.Local.LocationID,
		}
	})

	// 给上传节点的IPFS加锁
	ipfsReqBlder := reqbuilder.NewBuilder()
	// 如果本地的IPFS也是存储系统的一个节点，那么从本地上传时，需要加锁
	if stgglb.Local.NodeID != nil {
		ipfsReqBlder.IPFS().Buzy(*stgglb.Local.NodeID)
	}
	for _, node := range userNodes {
		if stgglb.Local.NodeID != nil && node.Node.NodeID == *stgglb.Local.NodeID {
			continue
		}

		ipfsReqBlder.IPFS().Buzy(node.Node.NodeID)
	}
	// TODO 考虑加Object的Create锁
	// 防止上传的副本被清除
	ipfsMutex, err := ipfsReqBlder.MutexLock(ctx.Distlock)
	if err != nil {
		return nil, fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer ipfsMutex.Unlock()

	rets, err := uploadAndUpdatePackage(createPkgResp.PackageID, t.objectIter, userNodes, t.nodeAffinity)
	if err != nil {
		return nil, err
	}

	return &CreatePackageResult{
		PackageID:     createPkgResp.PackageID,
		ObjectResults: rets,
	}, nil
}

// chooseUploadNode 选择一个上传文件的节点
// 1. 选择设置了亲和性的节点
// 2. 从与当前客户端相同地域的节点中随机选一个
// 3. 没有用的话从所有节点中随机选一个
func chooseUploadNode(nodes []UploadNodeInfo, nodeAffinity *cdssdk.NodeID) UploadNodeInfo {
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

func uploadAndUpdatePackage(packageID cdssdk.PackageID, objectIter iterator.UploadingObjectIterator, userNodes []UploadNodeInfo, nodeAffinity *cdssdk.NodeID) ([]ObjectUploadResult, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	// 为所有文件选择相同的上传节点
	uploadNode := chooseUploadNode(userNodes, nodeAffinity)

	var uploadRets []ObjectUploadResult
	//上传文件夹
	var adds []coormq.AddObjectEntry
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
			if err != nil {
				return fmt.Errorf("uploading file: %w", err)
			}

			uploadRets = append(uploadRets, ObjectUploadResult{
				Info:  objInfo,
				Error: err,
			})

			adds = append(adds, coormq.NewAddObjectEntry(objInfo.Path, objInfo.Size, fileHash, uploadNode.Node.NodeID))
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	_, err = coorCli.UpdatePackage(coormq.NewUpdatePackage(packageID, adds, nil))
	if err != nil {
		return nil, fmt.Errorf("updating package: %w", err)
	}

	return uploadRets, nil
}

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

func uploadToNode(file io.Reader, nodeIP string, grpcPort int) (string, error) {
	rpcCli, err := stgglb.AgentRPCPool.Acquire(nodeIP, grpcPort)
	if err != nil {
		return "", fmt.Errorf("new agent rpc client: %w", err)
	}
	defer rpcCli.Close()

	return rpcCli.SendIPFSFile(file)
}

func uploadToLocalIPFS(file io.Reader, nodeID cdssdk.NodeID, shouldPin bool) (string, error) {
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

func pinIPFSFile(nodeID cdssdk.NodeID, fileHash string) error {
	agtCli, err := stgglb.AgentMQPool.Acquire(nodeID)
	if err != nil {
		return fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agtCli)

	// 然后让最近节点pin本地上传的文件
	_, err = agtCli.PinObject(agtmq.ReqPinObject([]string{fileHash}, false))
	if err != nil {
		return fmt.Errorf("start pinning object: %w", err)
	}

	return nil
}
