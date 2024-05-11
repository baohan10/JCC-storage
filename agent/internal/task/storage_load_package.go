package task

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/bitmap"
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/common/utils/reflect2"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	"gitlink.org.cn/cloudream/storage/common/utils"
)

type StorageLoadPackage struct {
	FullOutputPath string

	userID       cdssdk.UserID
	packageID    cdssdk.PackageID
	storageID    cdssdk.StorageID
	pinnedBlocks []stgmod.ObjectBlock
}

func NewStorageLoadPackage(userID cdssdk.UserID, packageID cdssdk.PackageID, storageID cdssdk.StorageID) *StorageLoadPackage {
	return &StorageLoadPackage{
		userID:    userID,
		packageID: packageID,
		storageID: storageID,
	}
}
func (t *StorageLoadPackage) Execute(task *task.Task[TaskContext], ctx TaskContext, complete CompleteFn) {
	err := t.do(task, ctx)

	complete(err, CompleteOption{
		RemovingDelay: time.Minute,
	})
}

func (t *StorageLoadPackage) do(task *task.Task[TaskContext], ctx TaskContext) error {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return fmt.Errorf("new IPFS client: %w", err)
	}
	defer stgglb.IPFSPool.Release(ipfsCli)

	getStgResp, err := coorCli.GetStorage(coormq.ReqGetStorage(t.userID, t.storageID))
	if err != nil {
		return fmt.Errorf("request to coordinator: %w", err)
	}

	outputDirPath := utils.MakeStorageLoadPackagePath(getStgResp.Storage.Directory, t.userID, t.packageID)
	if err = os.MkdirAll(outputDirPath, 0755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}
	t.FullOutputPath = outputDirPath

	getObjectDetails, err := coorCli.GetPackageObjectDetails(coormq.ReqGetPackageObjectDetails(t.packageID))
	if err != nil {
		return fmt.Errorf("getting package object details: %w", err)
	}

	mutex, err := reqbuilder.NewBuilder().
		// 提前占位
		Metadata().StoragePackage().CreateOne(t.userID, t.storageID, t.packageID).
		// 保护在storage目录中下载的文件
		Storage().Buzy(t.storageID).
		// 保护下载文件时同时保存到IPFS的文件
		IPFS().Buzy(getStgResp.Storage.NodeID).
		MutexLock(ctx.distlock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	for _, obj := range getObjectDetails.Objects {
		err := t.downloadOne(coorCli, ipfsCli, outputDirPath, obj)
		if err != nil {
			return err
		}
	}

	_, err = coorCli.StoragePackageLoaded(coormq.NewStoragePackageLoaded(t.userID, t.storageID, t.packageID, t.pinnedBlocks))
	if err != nil {
		return fmt.Errorf("loading package to storage: %w", err)
	}

	// TODO 要防止下载的临时文件被删除
	return err
}

func (t *StorageLoadPackage) downloadOne(coorCli *coormq.Client, ipfsCli *ipfs.PoolClient, dir string, obj stgmod.ObjectDetail) error {
	var file io.ReadCloser

	switch red := obj.Object.Redundancy.(type) {
	case *cdssdk.NoneRedundancy:
		reader, err := t.downloadNoneOrRepObject(ipfsCli, obj)
		if err != nil {
			return fmt.Errorf("downloading object: %w", err)
		}
		file = reader

	case *cdssdk.RepRedundancy:
		reader, err := t.downloadNoneOrRepObject(ipfsCli, obj)
		if err != nil {
			return fmt.Errorf("downloading rep object: %w", err)
		}
		file = reader

	case *cdssdk.ECRedundancy:
		reader, pinnedBlocks, err := t.downloadECObject(coorCli, ipfsCli, obj, red)
		if err != nil {
			return fmt.Errorf("downloading ec object: %w", err)
		}
		file = reader
		t.pinnedBlocks = append(t.pinnedBlocks, pinnedBlocks...)

	default:
		return fmt.Errorf("unknow redundancy type: %v", reflect2.TypeOfValue(obj.Object.Redundancy))
	}
	defer file.Close()

	fullPath := filepath.Join(dir, obj.Object.Path)

	lastDirPath := filepath.Dir(fullPath)
	if err := os.MkdirAll(lastDirPath, 0755); err != nil {
		return fmt.Errorf("creating object last dir: %w", err)
	}

	outputFile, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("creating object file: %w", err)
	}
	defer outputFile.Close()

	if _, err := io.Copy(outputFile, file); err != nil {
		return fmt.Errorf("writting object to file: %w", err)
	}

	return nil
}

func (t *StorageLoadPackage) downloadNoneOrRepObject(ipfsCli *ipfs.PoolClient, obj stgmod.ObjectDetail) (io.ReadCloser, error) {
	if len(obj.Blocks) == 0 && len(obj.PinnedAt) == 0 {
		return nil, fmt.Errorf("no node has this object")
	}

	// 不管实际有没有成功
	ipfsCli.Pin(obj.Object.FileHash)

	file, err := ipfsCli.OpenRead(obj.Object.FileHash)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (t *StorageLoadPackage) downloadECObject(coorCli *coormq.Client, ipfsCli *ipfs.PoolClient, obj stgmod.ObjectDetail, ecRed *cdssdk.ECRedundancy) (io.ReadCloser, []stgmod.ObjectBlock, error) {
	allNodes, err := t.sortDownloadNodes(coorCli, obj)
	if err != nil {
		return nil, nil, err
	}
	bsc, blocks := t.getMinReadingBlockSolution(allNodes, ecRed.K)
	osc, _ := t.getMinReadingObjectSolution(allNodes, ecRed.K)
	if bsc < osc {
		var fileStrs []io.ReadCloser

		rs, err := ec.NewStreamRs(ecRed.K, ecRed.N, ecRed.ChunkSize)
		if err != nil {
			return nil, nil, fmt.Errorf("new rs: %w", err)
		}

		for i := range blocks {
			// 不管实际有没有成功
			ipfsCli.Pin(blocks[i].Block.FileHash)

			str, err := ipfsCli.OpenRead(blocks[i].Block.FileHash)
			if err != nil {
				for i -= 1; i >= 0; i-- {
					fileStrs[i].Close()
				}
				return nil, nil, fmt.Errorf("donwloading file: %w", err)
			}

			fileStrs = append(fileStrs, str)
		}

		fileReaders, filesCloser := io2.ToReaders(fileStrs)

		var indexes []int
		var pinnedBlocks []stgmod.ObjectBlock
		for _, b := range blocks {
			indexes = append(indexes, b.Block.Index)
			pinnedBlocks = append(pinnedBlocks, stgmod.ObjectBlock{
				ObjectID: b.Block.ObjectID,
				Index:    b.Block.Index,
				NodeID:   *stgglb.Local.NodeID,
				FileHash: b.Block.FileHash,
			})
		}

		outputs, outputsCloser := io2.ToReaders(rs.ReconstructData(fileReaders, indexes))
		return io2.AfterReadClosed(io2.Length(io2.ChunkedJoin(outputs, int(ecRed.ChunkSize)), obj.Object.Size), func(c io.ReadCloser) {
			filesCloser()
			outputsCloser()
		}), pinnedBlocks, nil
	}

	// bsc >= osc，如果osc是MaxFloat64，那么bsc也一定是，也就意味着没有足够块来恢复文件
	if osc == math.MaxFloat64 {
		return nil, nil, fmt.Errorf("no enough blocks to reconstruct the file, want %d, get only %d", ecRed.K, len(blocks))
	}

	// 如果是直接读取的文件，那么就不需要Pin文件块
	str, err := ipfsCli.OpenRead(obj.Object.FileHash)
	return str, nil, err
}

type downloadNodeInfo struct {
	Node         cdssdk.Node
	ObjectPinned bool
	Blocks       []stgmod.ObjectBlock
	Distance     float64
}

func (t *StorageLoadPackage) sortDownloadNodes(coorCli *coormq.Client, obj stgmod.ObjectDetail) ([]*downloadNodeInfo, error) {
	var nodeIDs []cdssdk.NodeID
	for _, id := range obj.PinnedAt {
		if !lo.Contains(nodeIDs, id) {
			nodeIDs = append(nodeIDs, id)
		}
	}
	for _, b := range obj.Blocks {
		if !lo.Contains(nodeIDs, b.NodeID) {
			nodeIDs = append(nodeIDs, b.NodeID)
		}
	}

	getNodes, err := coorCli.GetNodes(coormq.NewGetNodes(nodeIDs))
	if err != nil {
		return nil, fmt.Errorf("getting nodes: %w", err)
	}

	downloadNodeMap := make(map[cdssdk.NodeID]*downloadNodeInfo)
	for _, id := range obj.PinnedAt {
		node, ok := downloadNodeMap[id]
		if !ok {
			mod := *getNodes.GetNode(id)
			node = &downloadNodeInfo{
				Node:         mod,
				ObjectPinned: true,
				Distance:     t.getNodeDistance(mod),
			}
			downloadNodeMap[id] = node
		}

		node.ObjectPinned = true
	}

	for _, b := range obj.Blocks {
		node, ok := downloadNodeMap[b.NodeID]
		if !ok {
			mod := *getNodes.GetNode(b.NodeID)
			node = &downloadNodeInfo{
				Node:     mod,
				Distance: t.getNodeDistance(mod),
			}
			downloadNodeMap[b.NodeID] = node
		}

		node.Blocks = append(node.Blocks, b)
	}

	return sort2.Sort(lo.Values(downloadNodeMap), func(left, right *downloadNodeInfo) int {
		return sort2.Cmp(left.Distance, right.Distance)
	}), nil
}

type downloadBlock struct {
	Node  cdssdk.Node
	Block stgmod.ObjectBlock
}

func (t *StorageLoadPackage) getMinReadingBlockSolution(sortedNodes []*downloadNodeInfo, k int) (float64, []downloadBlock) {
	gotBlocksMap := bitmap.Bitmap64(0)
	var gotBlocks []downloadBlock
	dist := float64(0.0)
	for _, n := range sortedNodes {
		for _, b := range n.Blocks {
			if !gotBlocksMap.Get(b.Index) {
				gotBlocks = append(gotBlocks, downloadBlock{
					Node:  n.Node,
					Block: b,
				})
				gotBlocksMap.Set(b.Index, true)
				dist += n.Distance
			}

			if len(gotBlocks) >= k {
				return dist, gotBlocks
			}
		}
	}

	return math.MaxFloat64, gotBlocks
}

func (t *StorageLoadPackage) getMinReadingObjectSolution(sortedNodes []*downloadNodeInfo, k int) (float64, *cdssdk.Node) {
	dist := math.MaxFloat64
	var downloadNode *cdssdk.Node
	for _, n := range sortedNodes {
		if n.ObjectPinned && float64(k)*n.Distance < dist {
			dist = float64(k) * n.Distance
			downloadNode = &n.Node
		}
	}

	return dist, downloadNode
}

func (t *StorageLoadPackage) getNodeDistance(node cdssdk.Node) float64 {
	if stgglb.Local.NodeID != nil {
		if node.NodeID == *stgglb.Local.NodeID {
			return consts.NodeDistanceSameNode
		}
	}

	if node.LocationID == stgglb.Local.LocationID {
		return consts.NodeDistanceSameLocation
	}

	return consts.NodeDistanceOther
}
