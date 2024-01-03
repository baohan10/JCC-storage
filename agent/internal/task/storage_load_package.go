package task

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	"gitlink.org.cn/cloudream/common/pkgs/task"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	myio "gitlink.org.cn/cloudream/common/utils/io"
	myref "gitlink.org.cn/cloudream/common/utils/reflect"
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

	getStgResp, err := coorCli.GetStorageInfo(coormq.NewGetStorageInfo(t.userID, t.storageID))
	if err != nil {
		return fmt.Errorf("request to coordinator: %w", err)
	}

	outputDirPath := utils.MakeStorageLoadPackagePath(getStgResp.Directory, t.userID, t.packageID)
	if err = os.MkdirAll(outputDirPath, 0755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}
	t.FullOutputPath = outputDirPath

	getObjectDetails, err := coorCli.GetPackageObjectDetails(coormq.NewGetPackageObjectDetails(t.packageID))
	if err != nil {
		return fmt.Errorf("getting package object details: %w", err)
	}

	mutex, err := reqbuilder.NewBuilder().
		// 提前占位
		Metadata().StoragePackage().CreateOne(t.userID, t.storageID, t.packageID).
		// 保护在storage目录中下载的文件
		Storage().Buzy(t.storageID).
		// 保护下载文件时同时保存到IPFS的文件
		IPFS().Buzy(getStgResp.NodeID).
		MutexLock(ctx.distlock)
	if err != nil {
		return fmt.Errorf("acquire locks failed, err: %w", err)
	}
	defer mutex.Unlock()

	for _, obj := range getObjectDetails.Objects {
		err := t.downloadOne(ipfsCli, outputDirPath, obj)
		if err != nil {
			return err
		}
	}

	_, err = coorCli.StoragePackageLoaded(coormq.NewStoragePackageLoaded(t.userID, t.storageID, t.packageID, t.pinnedBlocks))
	if err != nil {
		return fmt.Errorf("loading package to storage: %w", err)
	}

	return err
}

func (t *StorageLoadPackage) downloadOne(ipfsCli *ipfs.PoolClient, dir string, obj stgmod.ObjectDetail) error {
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
		reader, pinnedBlocks, err := t.downloadECObject(ipfsCli, obj, red)
		if err != nil {
			return fmt.Errorf("downloading ec object: %w", err)
		}
		file = reader
		t.pinnedBlocks = append(t.pinnedBlocks, pinnedBlocks...)

	default:
		return fmt.Errorf("unknow redundancy type: %v", myref.TypeOfValue(obj.Object.Redundancy))
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
	if len(obj.Blocks) == 0 {
		return nil, fmt.Errorf("no node has this object")
	}

	// 异步pin，不管实际有没有成功
	go func() {
		ipfsCli.Pin(obj.Object.FileHash)
	}()

	file, err := ipfsCli.OpenRead(obj.Object.FileHash)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (t *StorageLoadPackage) downloadECObject(ipfsCli *ipfs.PoolClient, obj stgmod.ObjectDetail, ecRed *cdssdk.ECRedundancy) (io.ReadCloser, []stgmod.ObjectBlock, error) {
	var chosenBlocks []stgmod.GrouppedObjectBlock
	grpBlocks := obj.GroupBlocks()
	for i := range grpBlocks {
		if len(chosenBlocks) == ecRed.K {
			break
		}

		chosenBlocks = append(chosenBlocks, grpBlocks[i])
	}

	if len(chosenBlocks) < ecRed.K {
		return nil, nil, fmt.Errorf("no enough blocks to reconstruct the file, want %d, get only %d", ecRed.K, len(chosenBlocks))
	}

	var fileStrs []io.ReadCloser

	rs, err := ec.NewRs(ecRed.K, ecRed.N, ecRed.ChunkSize)
	if err != nil {
		return nil, nil, fmt.Errorf("new rs: %w", err)
	}

	for i := range chosenBlocks {
		// 异步pin，不管实际有没有成功
		go func() {
			ipfsCli.Pin(chosenBlocks[i].FileHash)
		}()

		str, err := ipfsCli.OpenRead(chosenBlocks[i].FileHash)
		if err != nil {
			for i -= 1; i >= 0; i-- {
				fileStrs[i].Close()
			}
			return nil, nil, fmt.Errorf("donwloading file: %w", err)
		}

		fileStrs = append(fileStrs, str)
	}

	fileReaders, filesCloser := myio.ToReaders(fileStrs)

	var indexes []int
	var pinnedBlocks []stgmod.ObjectBlock
	for _, b := range chosenBlocks {
		indexes = append(indexes, b.Index)
		pinnedBlocks = append(pinnedBlocks, stgmod.ObjectBlock{
			ObjectID: b.ObjectID,
			Index:    b.Index,
			NodeID:   *stgglb.Local.NodeID,
			FileHash: b.FileHash,
		})
	}

	outputs, outputsCloser := myio.ToReaders(rs.ReconstructData(fileReaders, indexes))
	return myio.AfterReadClosed(myio.Length(myio.ChunkedJoin(outputs, int(ecRed.ChunkSize)), obj.Object.Size), func(c io.ReadCloser) {
		filesCloser()
		outputsCloser()
	}), pinnedBlocks, nil
}
