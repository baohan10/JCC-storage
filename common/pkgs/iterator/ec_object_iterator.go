package iterator

import (
	"fmt"
	"io"
	"math/rand"

	"github.com/samber/lo"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"

	myio "gitlink.org.cn/cloudream/common/utils/io"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmodels "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type ECObjectIterator struct {
	OnClosing func()

	objects      []model.Object
	objectECData []stgmodels.ObjectECData
	currentIndex int
	inited       bool

	ecInfo      cdssdk.ECRedundancyInfo
	ec          model.Ec
	downloadCtx *DownloadContext
	cliLocation model.Location
}

func NewECObjectIterator(objects []model.Object, objectECData []stgmodels.ObjectECData, ecInfo cdssdk.ECRedundancyInfo, ec model.Ec, downloadCtx *DownloadContext) *ECObjectIterator {
	return &ECObjectIterator{
		objects:      objects,
		objectECData: objectECData,
		ecInfo:       ecInfo,
		ec:           ec,
		downloadCtx:  downloadCtx,
	}
}

func (i *ECObjectIterator) MoveNext() (*IterDownloadingObject, error) {
	// TODO 加锁
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	if !i.inited {
		i.inited = true

		findCliLocResp, err := coorCli.FindClientLocation(coormq.NewFindClientLocation(stgglb.Local.ExternalIP))
		if err != nil {
			return nil, fmt.Errorf("finding client location: %w", err)
		}
		i.cliLocation = findCliLocResp.Location
	}

	if i.currentIndex >= len(i.objects) {
		return nil, ErrNoMoreItem
	}

	item, err := i.doMove(coorCli)
	i.currentIndex++
	return item, err
}

func (iter *ECObjectIterator) doMove(coorCli *coormq.Client) (*IterDownloadingObject, error) {
	obj := iter.objects[iter.currentIndex]
	ecData := iter.objectECData[iter.currentIndex]

	//采取直接读，优先选内网节点
	var chosenNodes []DownloadNodeInfo
	var chosenBlocks []stgmodels.ObjectBlockData
	for i := range ecData.Blocks {
		if len(chosenBlocks) == iter.ec.EcK {
			break
		}

		// 块没有被任何节点缓存或者获取失败都没关系，只要能获取到k个块的信息就行

		if len(ecData.Blocks[i].NodeIDs) == 0 {
			continue
		}

		getNodesResp, err := coorCli.GetNodes(coormq.NewGetNodes(ecData.Blocks[i].NodeIDs))
		if err != nil {
			continue
		}

		downloadNodes := lo.Map(getNodesResp.Nodes, func(node model.Node, index int) DownloadNodeInfo {
			return DownloadNodeInfo{
				Node:           node,
				IsSameLocation: node.LocationID == iter.cliLocation.LocationID,
			}
		})

		chosenBlocks = append(chosenBlocks, ecData.Blocks[i])
		chosenNodes = append(chosenNodes, iter.chooseDownloadNode(downloadNodes))

	}

	if len(chosenBlocks) < iter.ec.EcK {
		return nil, fmt.Errorf("no enough blocks to reconstruct the file, want %d, get only %d", iter.ec.EcK, len(chosenBlocks))
	}

	reader, err := iter.downloadEcObject(iter.downloadCtx, obj.Size, chosenNodes, chosenBlocks)
	if err != nil {
		return nil, fmt.Errorf("ec read failed, err: %w", err)
	}

	return &IterDownloadingObject{
		Object: obj,
		File:   reader,
	}, nil
}

func (i *ECObjectIterator) Close() {
	if i.OnClosing != nil {
		i.OnClosing()
	}
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

func (iter *ECObjectIterator) downloadEcObject(ctx *DownloadContext, fileSize int64, nodes []DownloadNodeInfo, blocks []stgmodels.ObjectBlockData) (io.ReadCloser, error) {
	var fileStrs []io.ReadCloser

	rs, err := ec.NewRs(iter.ec.EcK, iter.ec.EcN, iter.ecInfo.ChunkSize)
	if err != nil {
		return nil, fmt.Errorf("new rs: %w", err)
	}

	for i := range blocks {
		str, err := downloadFile(ctx, nodes[i], blocks[i].FileHash)
		if err != nil {
			for i -= 1; i >= 0; i-- {
				fileStrs[i].Close()
			}
			return nil, fmt.Errorf("donwloading file: %w", err)
		}

		fileStrs = append(fileStrs, str)
	}

	fileReaders, filesCloser := myio.ToReaders(fileStrs)

	var indexes []int
	for _, b := range blocks {
		indexes = append(indexes, b.Index)
	}

	outputs, outputsCloser := myio.ToReaders(rs.ReconstructData(fileReaders, indexes))
	return myio.AfterReadClosed(myio.Length(myio.ChunkedJoin(outputs, int(iter.ecInfo.ChunkSize)), fileSize), func(c io.ReadCloser) {
		filesCloser()
		outputsCloser()
	}), nil
}
