package event

import (
	"fmt"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/sort"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/plans"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
	"gitlink.org.cn/cloudream/storage/scanner/internal/config"
)

const (
	monthHours = 30 * 24
	yearHours  = 365 * 24
)

type CheckPackageRedundancy struct {
	*scevt.CheckPackageRedundancy
}

func NewCheckPackageRedundancy(evt *scevt.CheckPackageRedundancy) *CheckPackageRedundancy {
	return &CheckPackageRedundancy{
		CheckPackageRedundancy: evt,
	}
}

type NodeLoadInfo struct {
	Node             model.Node
	LoadsRecentMonth int
	LoadsRecentYear  int
}

func (t *CheckPackageRedundancy) TryMerge(other Event) bool {
	event, ok := other.(*CheckPackageRedundancy)
	if !ok {
		return false
	}

	return event.PackageID == t.PackageID
}

func (t *CheckPackageRedundancy) Execute(execCtx ExecuteContext) {
	log := logger.WithType[CheckPackageRedundancy]("Event")
	log.Debugf("begin with %v", logger.FormatStruct(t.CheckPackageRedundancy))
	defer log.Debugf("end")

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		log.Warnf("new coordinator client: %s", err.Error())
		return
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getObjs, err := coorCli.GetPackageObjectDetails(coormq.NewGetPackageObjectDetails(t.PackageID))
	if err != nil {
		log.Warnf("getting package objects: %s", err.Error())
		return
	}

	getLogs, err := coorCli.GetPackageLoadLogDetails(coormq.ReqGetPackageLoadLogDetails(t.PackageID))
	if err != nil {
		log.Warnf("getting package load log details: %s", err.Error())
		return
	}

	getNodes, err := coorCli.GetNodes(coormq.NewGetNodes(nil))
	if err != nil {
		log.Warnf("getting all nodes: %s", err.Error())
		return
	}

	if len(getNodes.Nodes) == 0 {
		log.Warnf("no available nodes")
		return
	}

	allNodes := make(map[cdssdk.NodeID]*NodeLoadInfo)
	for _, node := range getNodes.Nodes {
		allNodes[node.NodeID] = &NodeLoadInfo{
			Node: node,
		}
	}

	for _, log := range getLogs.Logs {
		info, ok := allNodes[log.Storage.NodeID]
		if !ok {
			continue
		}

		sinceNow := time.Since(log.CreateTime)
		if sinceNow.Hours() < monthHours {
			info.LoadsRecentMonth++
		} else if sinceNow.Hours() < yearHours {
			info.LoadsRecentYear++
		}
	}

	var changedObjects []coormq.ChangeObjectRedundancyEntry

	defRep := cdssdk.DefaultRepRedundancy
	defEC := cdssdk.DefaultECRedundancy

	newRepNodes := t.chooseNewNodesForRep(&defRep, allNodes)
	newECNodes := t.chooseNewNodesForEC(&defEC, allNodes)

	for _, obj := range getObjs.Objects {
		var entry *coormq.ChangeObjectRedundancyEntry
		var err error

		shouldUseEC := obj.Object.Size > config.Cfg().ECFileSizeThreshold

		switch red := obj.Object.Redundancy.(type) {
		case *cdssdk.NoneRedundancy:
			if shouldUseEC {
				log.WithField("ObjectID", obj.Object.ObjectID).Debugf("redundancy: none -> ec")
				entry, err = t.noneToEC(obj, &defEC, newECNodes)
			} else {
				log.WithField("ObjectID", obj.Object.ObjectID).Debugf("redundancy: none -> rep")
				entry, err = t.noneToRep(obj, &defRep, newRepNodes)
			}

		case *cdssdk.RepRedundancy:
			if shouldUseEC {
				log.WithField("ObjectID", obj.Object.ObjectID).Debugf("redundancy: rep -> ec")
				entry, err = t.repToEC(obj, &defEC, newECNodes)
			} else {
				uploadNodes := t.rechooseNodesForRep(obj, red, allNodes)
				entry, err = t.repToRep(obj, &defRep, uploadNodes)
			}

		case *cdssdk.ECRedundancy:
			if shouldUseEC {
				uploadNodes := t.rechooseNodesForEC(obj, red, allNodes)
				entry, err = t.ecToEC(obj, red, &defEC, uploadNodes)
			} else {
				log.WithField("ObjectID", obj.Object.ObjectID).Debugf("redundancy: ec -> rep")
				entry, err = t.ecToRep(obj, red, &defRep, newRepNodes)
			}
		}

		if entry != nil {
			changedObjects = append(changedObjects, *entry)
		}

		if err != nil {
			log.WithField("ObjectID", obj.Object.ObjectID).Warnf("%s, its redundancy wont be changed", err.Error())
		}
	}

	if len(changedObjects) == 0 {
		return
	}

	_, err = coorCli.ChangeObjectRedundancy(coormq.ReqChangeObjectRedundancy(changedObjects))
	if err != nil {
		log.Warnf("requesting to change object redundancy: %s", err.Error())
		return
	}
}

func (t *CheckPackageRedundancy) chooseNewNodesForRep(red *cdssdk.RepRedundancy, allNodes map[cdssdk.NodeID]*NodeLoadInfo) []*NodeLoadInfo {
	sortedNodes := sort.Sort(lo.Values(allNodes), func(left *NodeLoadInfo, right *NodeLoadInfo) int {
		dm := right.LoadsRecentMonth - left.LoadsRecentMonth
		if dm != 0 {
			return dm
		}

		return right.LoadsRecentYear - left.LoadsRecentYear
	})

	return t.chooseSoManyNodes(red.RepCount, sortedNodes)
}

func (t *CheckPackageRedundancy) chooseNewNodesForEC(red *cdssdk.ECRedundancy, allNodes map[cdssdk.NodeID]*NodeLoadInfo) []*NodeLoadInfo {
	sortedNodes := sort.Sort(lo.Values(allNodes), func(left *NodeLoadInfo, right *NodeLoadInfo) int {
		dm := right.LoadsRecentMonth - left.LoadsRecentMonth
		if dm != 0 {
			return dm
		}

		return right.LoadsRecentYear - left.LoadsRecentYear
	})

	return t.chooseSoManyNodes(red.N, sortedNodes)
}

func (t *CheckPackageRedundancy) rechooseNodesForRep(obj stgmod.ObjectDetail, red *cdssdk.RepRedundancy, allNodes map[cdssdk.NodeID]*NodeLoadInfo) []*NodeLoadInfo {
	type rechooseNode struct {
		*NodeLoadInfo
		CachedBlockIndex int
	}

	var rechooseNodes []*rechooseNode
	for _, node := range allNodes {
		cachedBlockIndex := -1
		for _, block := range obj.Blocks {
			if lo.Contains(block.CachedNodeIDs, node.Node.NodeID) {
				cachedBlockIndex = block.Index
				break
			}
		}

		rechooseNodes = append(rechooseNodes, &rechooseNode{
			NodeLoadInfo:     node,
			CachedBlockIndex: cachedBlockIndex,
		})
	}

	sortedNodes := sort.Sort(rechooseNodes, func(left *rechooseNode, right *rechooseNode) int {
		dm := right.LoadsRecentMonth - left.LoadsRecentMonth
		if dm != 0 {
			return dm
		}

		// 已经缓存了文件块的节点优先选择
		v := sort.CmpBool(right.CachedBlockIndex > -1, left.CachedBlockIndex > -1)
		if v != 0 {
			return v
		}

		return right.LoadsRecentYear - left.LoadsRecentYear
	})

	return t.chooseSoManyNodes(red.RepCount, lo.Map(sortedNodes, func(node *rechooseNode, idx int) *NodeLoadInfo { return node.NodeLoadInfo }))
}

func (t *CheckPackageRedundancy) rechooseNodesForEC(obj stgmod.ObjectDetail, red *cdssdk.ECRedundancy, allNodes map[cdssdk.NodeID]*NodeLoadInfo) []*NodeLoadInfo {
	type rechooseNode struct {
		*NodeLoadInfo
		CachedBlockIndex int
	}

	var rechooseNodes []*rechooseNode
	for _, node := range allNodes {
		cachedBlockIndex := -1
		for _, block := range obj.Blocks {
			if lo.Contains(block.CachedNodeIDs, node.Node.NodeID) {
				cachedBlockIndex = block.Index
				break
			}
		}

		rechooseNodes = append(rechooseNodes, &rechooseNode{
			NodeLoadInfo:     node,
			CachedBlockIndex: cachedBlockIndex,
		})
	}

	sortedNodes := sort.Sort(rechooseNodes, func(left *rechooseNode, right *rechooseNode) int {
		dm := right.LoadsRecentMonth - left.LoadsRecentMonth
		if dm != 0 {
			return dm
		}

		// 已经缓存了文件块的节点优先选择
		v := sort.CmpBool(right.CachedBlockIndex > -1, left.CachedBlockIndex > -1)
		if v != 0 {
			return v
		}

		return right.LoadsRecentYear - left.LoadsRecentYear
	})

	// TODO 可以考虑选择已有块的节点时，能依然按照Index顺序选择
	return t.chooseSoManyNodes(red.N, lo.Map(sortedNodes, func(node *rechooseNode, idx int) *NodeLoadInfo { return node.NodeLoadInfo }))
}

func (t *CheckPackageRedundancy) chooseSoManyNodes(count int, nodes []*NodeLoadInfo) []*NodeLoadInfo {
	repeateCount := (count + len(nodes) - 1) / len(nodes)
	extedNodes := make([]*NodeLoadInfo, repeateCount*len(nodes))

	// 使用复制的方式将节点数扩充到要求的数量
	// 复制之后的结构：ABCD -> AAABBBCCCDDD
	for p := 0; p < repeateCount; p++ {
		for i, node := range nodes {
			putIdx := i*repeateCount + p
			extedNodes[putIdx] = node
		}
	}
	extedNodes = extedNodes[:count]

	var chosen []*NodeLoadInfo
	for len(chosen) < count {
		// 在每一轮内都选不同地区的节点，如果节点数不够，那么就再来一轮
		chosenLocations := make(map[cdssdk.LocationID]bool)
		for i, node := range extedNodes {
			if node == nil {
				continue
			}

			if chosenLocations[node.Node.LocationID] {
				continue
			}

			chosen = append(chosen, node)
			chosenLocations[node.Node.LocationID] = true
			extedNodes[i] = nil
		}
	}

	return chosen
}

func (t *CheckPackageRedundancy) noneToRep(obj stgmod.ObjectDetail, red *cdssdk.RepRedundancy, uploadNodes []*NodeLoadInfo) (*coormq.ChangeObjectRedundancyEntry, error) {
	if len(obj.CachedNodeIDs) == 0 {
		return nil, fmt.Errorf("object is not cached on any nodes, cannot change its redundancy to rep")
	}

	// 如果选择的备份节点都是同一个，那么就只要上传一次
	uploadNodes = lo.UniqBy(uploadNodes, func(item *NodeLoadInfo) cdssdk.NodeID { return item.Node.NodeID })

	var blocks []stgmod.ObjectBlock
	for _, node := range uploadNodes {
		err := t.pinObject(node.Node.NodeID, obj.Object.FileHash)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, stgmod.ObjectBlock{
			ObjectID: obj.Object.ObjectID,
			Index:    0,
			NodeID:   node.Node.NodeID,
			FileHash: obj.Object.FileHash,
		})
	}

	return &coormq.ChangeObjectRedundancyEntry{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: red,
		Blocks:     blocks,
	}, nil
}

func (t *CheckPackageRedundancy) noneToEC(obj stgmod.ObjectDetail, red *cdssdk.ECRedundancy, uploadNodes []*NodeLoadInfo) (*coormq.ChangeObjectRedundancyEntry, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	if len(obj.CachedNodeIDs) == 0 {
		return nil, fmt.Errorf("object is not cached on any nodes, cannot change its redundancy to ec")
	}

	getNodes, err := coorCli.GetNodes(coormq.NewGetNodes([]cdssdk.NodeID{obj.CachedNodeIDs[0]}))
	if err != nil {
		return nil, fmt.Errorf("requesting to get nodes: %w", err)
	}

	planBlder := plans.NewPlanBuilder()
	inputStrs := planBlder.AtAgent(getNodes.Nodes[0]).IPFSRead(obj.Object.FileHash).ChunkedSplit(red.ChunkSize, red.K, true)
	outputStrs := planBlder.AtAgent(getNodes.Nodes[0]).ECReconstructAny(*red, lo.Range(red.K), lo.Range(red.N), inputStrs.Streams...)
	for i := 0; i < red.N; i++ {
		outputStrs.Stream(i).GRPCSend(uploadNodes[i].Node).IPFSWrite(fmt.Sprintf("%d", i))
	}
	plan, err := planBlder.Build()
	if err != nil {
		return nil, fmt.Errorf("building io plan: %w", err)
	}

	exec, err := plans.Execute(*plan)
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	ioRet, err := exec.Wait()
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	var blocks []stgmod.ObjectBlock
	for i := 0; i < red.N; i++ {
		blocks = append(blocks, stgmod.ObjectBlock{
			ObjectID: obj.Object.ObjectID,
			Index:    i,
			NodeID:   uploadNodes[i].Node.NodeID,
			FileHash: ioRet.ResultValues[fmt.Sprintf("%d", i)].(string),
		})
	}

	return &coormq.ChangeObjectRedundancyEntry{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: red,
		Blocks:     blocks,
	}, nil
}

func (t *CheckPackageRedundancy) repToRep(obj stgmod.ObjectDetail, red *cdssdk.RepRedundancy, uploadNodes []*NodeLoadInfo) (*coormq.ChangeObjectRedundancyEntry, error) {
	if len(obj.CachedNodeIDs) == 0 {
		return nil, fmt.Errorf("object is not cached on any nodes, cannot change its redundancy to rep")
	}

	// 如果选择的备份节点都是同一个，那么就只要上传一次
	uploadNodes = lo.UniqBy(uploadNodes, func(item *NodeLoadInfo) cdssdk.NodeID { return item.Node.NodeID })

	for _, node := range uploadNodes {
		err := t.pinObject(node.Node.NodeID, obj.Object.FileHash)
		if err != nil {
			logger.WithField("ObjectID", obj.Object.ObjectID).
				Warn(err.Error())
			return nil, err
		}
	}

	var blocks []stgmod.ObjectBlock
	for _, node := range uploadNodes {
		// 由于更新冗余方式会删除所有Block记录然后重新填充，
		// 所以即使是节点跳过了上传，也需要为它添加一条Block记录
		blocks = append(blocks, stgmod.ObjectBlock{
			ObjectID: obj.Object.ObjectID,
			Index:    0,
			NodeID:   node.Node.NodeID,
			FileHash: obj.Object.FileHash,
		})
	}

	return &coormq.ChangeObjectRedundancyEntry{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: red,
		Blocks:     blocks,
	}, nil
}

func (t *CheckPackageRedundancy) repToEC(obj stgmod.ObjectDetail, red *cdssdk.ECRedundancy, uploadNodes []*NodeLoadInfo) (*coormq.ChangeObjectRedundancyEntry, error) {
	return t.noneToEC(obj, red, uploadNodes)
}

func (t *CheckPackageRedundancy) ecToRep(obj stgmod.ObjectDetail, srcRed *cdssdk.ECRedundancy, tarRed *cdssdk.RepRedundancy, uploadNodes []*NodeLoadInfo) (*coormq.ChangeObjectRedundancyEntry, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	var chosenBlocks []stgmod.ObjectBlockDetail
	var chosenBlockIndexes []int
	for _, block := range obj.Blocks {
		if len(block.CachedNodeIDs) > 0 {
			chosenBlocks = append(chosenBlocks, block)
			chosenBlockIndexes = append(chosenBlockIndexes, block.Index)
		}

		if len(chosenBlocks) == srcRed.K {
			break
		}
	}

	if len(chosenBlocks) < srcRed.K {
		return nil, fmt.Errorf("no enough blocks to reconstruct the original file data")
	}

	// 如果选择的备份节点都是同一个，那么就只要上传一次
	uploadNodes = lo.UniqBy(uploadNodes, func(item *NodeLoadInfo) cdssdk.NodeID { return item.Node.NodeID })

	// 每个被选节点都在自己节点上重建原始数据
	planBlder := plans.NewPlanBuilder()
	for i := range uploadNodes {
		tarNode := planBlder.AtAgent(uploadNodes[i].Node)

		var inputs []*plans.AgentStream
		for _, block := range chosenBlocks {
			inputs = append(inputs, tarNode.IPFSRead(block.FileHash))
		}

		outputs := tarNode.ECReconstruct(*srcRed, chosenBlockIndexes, inputs...)
		tarNode.ChunkedJoin(srcRed.ChunkSize, outputs.Streams...).Length(obj.Object.Size).IPFSWrite(fmt.Sprintf("%d", i))
	}

	plan, err := planBlder.Build()
	if err != nil {
		return nil, fmt.Errorf("building io plan: %w", err)
	}

	exec, err := plans.Execute(*plan)
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	ioRet, err := exec.Wait()
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	var blocks []stgmod.ObjectBlock
	for i := range uploadNodes {
		blocks = append(blocks, stgmod.ObjectBlock{
			ObjectID: obj.Object.ObjectID,
			Index:    0,
			NodeID:   uploadNodes[i].Node.NodeID,
			FileHash: ioRet.ResultValues[fmt.Sprintf("%d", i)].(string),
		})
	}

	return &coormq.ChangeObjectRedundancyEntry{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: tarRed,
		Blocks:     blocks,
	}, nil
}

func (t *CheckPackageRedundancy) ecToEC(obj stgmod.ObjectDetail, srcRed *cdssdk.ECRedundancy, tarRed *cdssdk.ECRedundancy, uploadNodes []*NodeLoadInfo) (*coormq.ChangeObjectRedundancyEntry, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	var chosenBlocks []stgmod.ObjectBlockDetail
	var chosenBlockIndexes []int
	for _, block := range obj.Blocks {
		if len(block.CachedNodeIDs) > 0 {
			chosenBlocks = append(chosenBlocks, block)
			chosenBlockIndexes = append(chosenBlockIndexes, block.Index)
		}

		if len(chosenBlocks) == srcRed.K {
			break
		}
	}

	if len(chosenBlocks) < srcRed.K {
		return nil, fmt.Errorf("no enough blocks to reconstruct the original file data")
	}

	// 目前EC的参数都相同，所以可以不用重建出完整数据然后再分块，可以直接构建出目的节点需要的块
	planBlder := plans.NewPlanBuilder()

	var newBlocks []stgmod.ObjectBlock
	shouldUpdateBlocks := false
	for i := range obj.Blocks {
		newBlocks = append(newBlocks, stgmod.ObjectBlock{
			ObjectID: obj.Object.ObjectID,
			Index:    i,
			NodeID:   uploadNodes[i].Node.NodeID,
			FileHash: obj.Blocks[i].FileHash,
		})

		// 如果新选中的节点已经记录在Block表中，那么就不需要任何变更
		if lo.Contains(obj.Blocks[i].NodeIDs, uploadNodes[i].Node.NodeID) {
			continue
		}

		shouldUpdateBlocks = true

		// 新选的节点不在Block表中，但实际上保存了分块的数据，那么只需建立一条Block记录即可
		if lo.Contains(obj.Blocks[i].CachedNodeIDs, uploadNodes[i].Node.NodeID) {
			continue
		}

		// 否则就要重建出这个节点需要的块
		tarNode := planBlder.AtAgent(uploadNodes[i].Node)

		var inputs []*plans.AgentStream
		for _, block := range chosenBlocks {
			inputs = append(inputs, tarNode.IPFSRead(block.FileHash))
		}

		// 输出只需要自己要保存的那一块
		tarNode.ECReconstructAny(*srcRed, chosenBlockIndexes, []int{i}, inputs...).Stream(0).IPFSWrite("")
	}

	plan, err := planBlder.Build()
	if err != nil {
		return nil, fmt.Errorf("building io plan: %w", err)
	}

	exec, err := plans.Execute(*plan)
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	// 如果没有任何Plan，Wait会直接返回成功
	_, err = exec.Wait()
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	if !shouldUpdateBlocks {
		return nil, nil
	}

	return &coormq.ChangeObjectRedundancyEntry{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: tarRed,
		Blocks:     newBlocks,
	}, nil
}

func (t *CheckPackageRedundancy) pinObject(nodeID cdssdk.NodeID, fileHash string) error {
	agtCli, err := stgglb.AgentMQPool.Acquire(nodeID)
	if err != nil {
		return fmt.Errorf("new agent client: %w", err)
	}
	defer stgglb.AgentMQPool.Release(agtCli)

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

func init() {
	RegisterMessageConvertor(NewCheckPackageRedundancy)
}
