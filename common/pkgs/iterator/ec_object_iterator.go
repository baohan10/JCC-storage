package iterator

import (
	"fmt"
	"io"
	"math/rand"
	"os"

	"github.com/samber/lo"

	"gitlink.org.cn/cloudream/common/pkgs/logger"
	stgsdk "gitlink.org.cn/cloudream/common/sdks/storage"

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

	ecInfo      stgsdk.ECRedundancyInfo
	ec          model.Ec
	downloadCtx *DownloadContext
	cliLocation model.Location
}

func NewECObjectIterator(objects []model.Object, objectECData []stgmodels.ObjectECData, ecInfo stgsdk.ECRedundancyInfo, ec model.Ec, downloadCtx *DownloadContext) *ECObjectIterator {
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

	blocks := ecData.Blocks
	ec := iter.ec
	ecK := ec.EcK
	ecN := ec.EcN
	//采取直接读，优先选内网节点
	hashs := make([]string, ecK)
	nds := make([]DownloadNodeInfo, ecK)
	for i := 0; i < ecK; i++ {
		hashs[i] = blocks[i].FileHash

		getNodesResp, err := coorCli.GetNodes(coormq.NewGetNodes(blocks[i].NodeIDs))
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
			logger.Infof("client and node %d are at the same location, use local ip", nds[i].Node.NodeID)
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

func (iter *ECObjectIterator) downloadEcObject(fileSize int64, ecK int, ecN int, blockIDs []int, nodeIDs []int64, nodeIPs []string, hashs []string) (io.ReadCloser, error) {
	// TODO zkx 先试用同步方式实现逻辑，做好错误处理。同时也方便下面直接使用uploadToNode和uploadToLocalIPFS来优化代码结构
	//wg := sync.WaitGroup{}
	numPacket := (fileSize + int64(ecK)*iter.ecInfo.PacketSize - 1) / (int64(ecK) * iter.ecInfo.PacketSize)
	getBufs := make([]chan []byte, ecN)
	decodeBufs := make([]chan []byte, ecK)
	for i := 0; i < ecN; i++ {
		getBufs[i] = make(chan []byte)
	}
	for i := 0; i < ecK; i++ {
		decodeBufs[i] = make(chan []byte)
	}
	for idx := 0; idx < len(blockIDs); idx++ {
		i := idx
		go func() {
			// TODO 处理错误
			file, _ := downloadFile(iter.downloadCtx, nodeIDs[i], nodeIPs[i], hashs[i])

			for p := int64(0); p < numPacket; p++ {
				buf := make([]byte, iter.ecInfo.PacketSize)
				// TODO 处理错误
				io.ReadFull(file, buf)
				getBufs[blockIDs[i]] <- buf
			}
		}()
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
