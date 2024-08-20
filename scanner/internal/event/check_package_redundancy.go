package event

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/parser"
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
	Node             cdssdk.Node
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
	startTime := time.Now()
	log.Debugf("begin with %v", logger.FormatStruct(t.CheckPackageRedundancy))
	defer func() {
		log.Debugf("end, time: %v", time.Since(startTime))
	}()

	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		log.Warnf("new coordinator client: %s", err.Error())
		return
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	getObjs, err := coorCli.GetPackageObjectDetails(coormq.ReqGetPackageObjectDetails(t.PackageID))
	if err != nil {
		log.Warnf("getting package objects: %s", err.Error())
		return
	}

	getLogs, err := coorCli.GetPackageLoadLogDetails(coormq.ReqGetPackageLoadLogDetails(t.PackageID))
	if err != nil {
		log.Warnf("getting package load log details: %s", err.Error())
		return
	}

	// TODO UserID
	getNodes, err := coorCli.GetUserNodes(coormq.NewGetUserNodes(1))
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

	var changedObjects []coormq.UpdatingObjectRedundancy

	defRep := cdssdk.DefaultRepRedundancy
	defEC := cdssdk.DefaultECRedundancy

	// TODO 目前rep的备份数量固定为2，所以这里直接选出两个节点
	mostBlockNodeIDs := t.summaryRepObjectBlockNodes(getObjs.Objects, 2)
	newRepNodes := t.chooseNewNodesForRep(&defRep, allNodes)
	rechoosedRepNodes := t.rechooseNodesForRep(mostBlockNodeIDs, &defRep, allNodes)
	newECNodes := t.chooseNewNodesForEC(&defEC, allNodes)

	// 加锁
	builder := reqbuilder.NewBuilder()
	for _, node := range newRepNodes {
		builder.IPFS().Buzy(node.Node.NodeID)
	}
	for _, node := range newECNodes {
		builder.IPFS().Buzy(node.Node.NodeID)
	}
	mutex, err := builder.MutexLock(execCtx.Args.DistLock)
	if err != nil {
		log.Warnf("acquiring dist lock: %s", err.Error())
		return
	}
	defer mutex.Unlock()

	for _, obj := range getObjs.Objects {
		var updating *coormq.UpdatingObjectRedundancy
		var err error

		shouldUseEC := obj.Object.Size > config.Cfg().ECFileSizeThreshold

		switch red := obj.Object.Redundancy.(type) {
		case *cdssdk.NoneRedundancy:
			if shouldUseEC {
				log.WithField("ObjectID", obj.Object.ObjectID).Debugf("redundancy: none -> ec")
				updating, err = t.noneToEC(obj, &defEC, newECNodes)
			} else {
				log.WithField("ObjectID", obj.Object.ObjectID).Debugf("redundancy: none -> rep")
				updating, err = t.noneToRep(obj, &defRep, newRepNodes)
			}

		case *cdssdk.RepRedundancy:
			if shouldUseEC {
				log.WithField("ObjectID", obj.Object.ObjectID).Debugf("redundancy: rep -> ec")
				updating, err = t.repToEC(obj, &defEC, newECNodes)
			} else {
				updating, err = t.repToRep(obj, &defRep, rechoosedRepNodes)
			}

		case *cdssdk.ECRedundancy:
			if shouldUseEC {
				uploadNodes := t.rechooseNodesForEC(obj, red, allNodes)
				updating, err = t.ecToEC(obj, red, &defEC, uploadNodes)
			} else {
				log.WithField("ObjectID", obj.Object.ObjectID).Debugf("redundancy: ec -> rep")
				updating, err = t.ecToRep(obj, red, &defRep, newRepNodes)
			}
		}

		if updating != nil {
			changedObjects = append(changedObjects, *updating)
		}

		if err != nil {
			log.WithField("ObjectID", obj.Object.ObjectID).Warnf("%s, its redundancy wont be changed", err.Error())
		}
	}

	if len(changedObjects) == 0 {
		return
	}

	_, err = coorCli.UpdateObjectRedundancy(coormq.ReqUpdateObjectRedundancy(changedObjects))
	if err != nil {
		log.Warnf("requesting to change object redundancy: %s", err.Error())
		return
	}
}

// 统计每个对象块所在的节点，选出块最多的不超过nodeCnt个节点
func (t *CheckPackageRedundancy) summaryRepObjectBlockNodes(objs []stgmod.ObjectDetail, nodeCnt int) []cdssdk.NodeID {
	type nodeBlocks struct {
		NodeID cdssdk.NodeID
		Count  int
	}

	nodeBlocksMap := make(map[cdssdk.NodeID]*nodeBlocks)
	for _, obj := range objs {
		shouldUseEC := obj.Object.Size > config.Cfg().ECFileSizeThreshold
		if _, ok := obj.Object.Redundancy.(*cdssdk.RepRedundancy); ok && !shouldUseEC {
			for _, block := range obj.Blocks {
				if _, ok := nodeBlocksMap[block.NodeID]; !ok {
					nodeBlocksMap[block.NodeID] = &nodeBlocks{
						NodeID: block.NodeID,
						Count:  0,
					}
				}
				nodeBlocksMap[block.NodeID].Count++
			}
		}
	}

	nodes := lo.Values(nodeBlocksMap)
	sort2.Sort(nodes, func(left *nodeBlocks, right *nodeBlocks) int {
		return right.Count - left.Count
	})

	ids := lo.Map(nodes, func(item *nodeBlocks, idx int) cdssdk.NodeID { return item.NodeID })
	if len(ids) > nodeCnt {
		ids = ids[:nodeCnt]
	}
	return ids
}

func (t *CheckPackageRedundancy) chooseNewNodesForRep(red *cdssdk.RepRedundancy, allNodes map[cdssdk.NodeID]*NodeLoadInfo) []*NodeLoadInfo {
	sortedNodes := sort2.Sort(lo.Values(allNodes), func(left *NodeLoadInfo, right *NodeLoadInfo) int {
		dm := right.LoadsRecentMonth - left.LoadsRecentMonth
		if dm != 0 {
			return dm
		}

		return right.LoadsRecentYear - left.LoadsRecentYear
	})

	return t.chooseSoManyNodes(red.RepCount, sortedNodes)
}

func (t *CheckPackageRedundancy) chooseNewNodesForEC(red *cdssdk.ECRedundancy, allNodes map[cdssdk.NodeID]*NodeLoadInfo) []*NodeLoadInfo {
	sortedNodes := sort2.Sort(lo.Values(allNodes), func(left *NodeLoadInfo, right *NodeLoadInfo) int {
		dm := right.LoadsRecentMonth - left.LoadsRecentMonth
		if dm != 0 {
			return dm
		}

		return right.LoadsRecentYear - left.LoadsRecentYear
	})

	return t.chooseSoManyNodes(red.N, sortedNodes)
}

func (t *CheckPackageRedundancy) rechooseNodesForRep(mostBlockNodeIDs []cdssdk.NodeID, red *cdssdk.RepRedundancy, allNodes map[cdssdk.NodeID]*NodeLoadInfo) []*NodeLoadInfo {
	type rechooseNode struct {
		*NodeLoadInfo
		HasBlock bool
	}

	var rechooseNodes []*rechooseNode
	for _, node := range allNodes {
		hasBlock := false
		for _, id := range mostBlockNodeIDs {
			if id == node.Node.NodeID {
				hasBlock = true
				break
			}
		}

		rechooseNodes = append(rechooseNodes, &rechooseNode{
			NodeLoadInfo: node,
			HasBlock:     hasBlock,
		})
	}

	sortedNodes := sort2.Sort(rechooseNodes, func(left *rechooseNode, right *rechooseNode) int {
		dm := right.LoadsRecentMonth - left.LoadsRecentMonth
		if dm != 0 {
			return dm
		}

		// 已经缓存了文件块的节点优先选择
		v := sort2.CmpBool(right.HasBlock, left.HasBlock)
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
			if block.NodeID == node.Node.NodeID {
				cachedBlockIndex = block.Index
				break
			}
		}

		rechooseNodes = append(rechooseNodes, &rechooseNode{
			NodeLoadInfo:     node,
			CachedBlockIndex: cachedBlockIndex,
		})
	}

	sortedNodes := sort2.Sort(rechooseNodes, func(left *rechooseNode, right *rechooseNode) int {
		dm := right.LoadsRecentMonth - left.LoadsRecentMonth
		if dm != 0 {
			return dm
		}

		// 已经缓存了文件块的节点优先选择
		v := sort2.CmpBool(right.CachedBlockIndex > -1, left.CachedBlockIndex > -1)
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

func (t *CheckPackageRedundancy) noneToRep(obj stgmod.ObjectDetail, red *cdssdk.RepRedundancy, uploadNodes []*NodeLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
	if len(obj.Blocks) == 0 {
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

	return &coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: red,
		Blocks:     blocks,
	}, nil
}

func (t *CheckPackageRedundancy) noneToEC(obj stgmod.ObjectDetail, red *cdssdk.ECRedundancy, uploadNodes []*NodeLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	if len(obj.Blocks) == 0 {
		return nil, fmt.Errorf("object is not cached on any nodes, cannot change its redundancy to ec")
	}

	getNodes, err := coorCli.GetNodes(coormq.NewGetNodes([]cdssdk.NodeID{obj.Blocks[0].NodeID}))
	if err != nil {
		return nil, fmt.Errorf("requesting to get nodes: %w", err)
	}

	ft := ioswitch2.NewFromTo()
	ft.AddFrom(ioswitch2.NewFromNode(obj.Object.FileHash, &getNodes.Nodes[0], -1))
	for i := 0; i < red.N; i++ {
		ft.AddTo(ioswitch2.NewToNode(uploadNodes[i].Node, i, fmt.Sprintf("%d", i)))
	}
	parser := parser.NewParser(*red)
	plans := exec.NewPlanBuilder()
	err = parser.Parse(ft, plans)
	if err != nil {
		return nil, fmt.Errorf("parsing plan: %w", err)
	}

	ioRet, err := plans.Execute().Wait(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	var blocks []stgmod.ObjectBlock
	for i := 0; i < red.N; i++ {
		blocks = append(blocks, stgmod.ObjectBlock{
			ObjectID: obj.Object.ObjectID,
			Index:    i,
			NodeID:   uploadNodes[i].Node.NodeID,
			FileHash: ioRet[fmt.Sprintf("%d", i)].(string),
		})
	}

	return &coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: red,
		Blocks:     blocks,
	}, nil
}

func (t *CheckPackageRedundancy) repToRep(obj stgmod.ObjectDetail, red *cdssdk.RepRedundancy, uploadNodes []*NodeLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
	if len(obj.Blocks) == 0 {
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

	return &coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: red,
		Blocks:     blocks,
	}, nil
}

func (t *CheckPackageRedundancy) repToEC(obj stgmod.ObjectDetail, red *cdssdk.ECRedundancy, uploadNodes []*NodeLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
	return t.noneToEC(obj, red, uploadNodes)
}

func (t *CheckPackageRedundancy) ecToRep(obj stgmod.ObjectDetail, srcRed *cdssdk.ECRedundancy, tarRed *cdssdk.RepRedundancy, uploadNodes []*NodeLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	var chosenBlocks []stgmod.GrouppedObjectBlock
	var chosenBlockIndexes []int
	for _, block := range obj.GroupBlocks() {
		if len(block.NodeIDs) > 0 {
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
	parser := parser.NewParser(*srcRed)
	planBlder := exec.NewPlanBuilder()
	for i := range uploadNodes {
		ft := ioswitch2.NewFromTo()

		for _, block := range chosenBlocks {
			ft.AddFrom(ioswitch2.NewFromNode(block.FileHash, &uploadNodes[i].Node, block.Index))
		}

		len := obj.Object.Size
		ft.AddTo(ioswitch2.NewToNodeWithRange(uploadNodes[i].Node, -1, fmt.Sprintf("%d", i), exec.Range{
			Offset: 0,
			Length: &len,
		}))

		err := parser.Parse(ft, planBlder)
		if err != nil {
			return nil, fmt.Errorf("parsing plan: %w", err)
		}
	}

	ioRet, err := planBlder.Execute().Wait(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	var blocks []stgmod.ObjectBlock
	for i := range uploadNodes {
		blocks = append(blocks, stgmod.ObjectBlock{
			ObjectID: obj.Object.ObjectID,
			Index:    0,
			NodeID:   uploadNodes[i].Node.NodeID,
			FileHash: ioRet[fmt.Sprintf("%d", i)].(string),
		})
	}

	return &coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: tarRed,
		Blocks:     blocks,
	}, nil
}

func (t *CheckPackageRedundancy) ecToEC(obj stgmod.ObjectDetail, srcRed *cdssdk.ECRedundancy, tarRed *cdssdk.ECRedundancy, uploadNodes []*NodeLoadInfo) (*coormq.UpdatingObjectRedundancy, error) {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return nil, fmt.Errorf("new coordinator client: %w", err)
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	grpBlocks := obj.GroupBlocks()

	var chosenBlocks []stgmod.GrouppedObjectBlock
	for _, block := range grpBlocks {
		if len(block.NodeIDs) > 0 {
			chosenBlocks = append(chosenBlocks, block)
		}

		if len(chosenBlocks) == srcRed.K {
			break
		}
	}

	if len(chosenBlocks) < srcRed.K {
		return nil, fmt.Errorf("no enough blocks to reconstruct the original file data")
	}

	// 目前EC的参数都相同，所以可以不用重建出完整数据然后再分块，可以直接构建出目的节点需要的块
	parser := parser.NewParser(*srcRed)
	planBlder := exec.NewPlanBuilder()

	var newBlocks []stgmod.ObjectBlock
	shouldUpdateBlocks := false
	for i, node := range uploadNodes {
		newBlock := stgmod.ObjectBlock{
			ObjectID: obj.Object.ObjectID,
			Index:    i,
			NodeID:   node.Node.NodeID,
		}

		grp, ok := lo.Find(grpBlocks, func(grp stgmod.GrouppedObjectBlock) bool { return grp.Index == i })

		// 如果新选中的节点已经记录在Block表中，那么就不需要任何变更
		if ok && lo.Contains(grp.NodeIDs, node.Node.NodeID) {
			newBlock.FileHash = grp.FileHash
			newBlocks = append(newBlocks, newBlock)
			continue
		}

		shouldUpdateBlocks = true

		// 否则就要重建出这个节点需要的块

		ft := ioswitch2.NewFromTo()
		for _, block := range chosenBlocks {
			ft.AddFrom(ioswitch2.NewFromNode(block.FileHash, &node.Node, block.Index))
		}

		// 输出只需要自己要保存的那一块
		ft.AddTo(ioswitch2.NewToNode(node.Node, i, fmt.Sprintf("%d", i)))

		err := parser.Parse(ft, planBlder)
		if err != nil {
			return nil, fmt.Errorf("parsing plan: %w", err)
		}

		newBlocks = append(newBlocks, newBlock)
	}

	// 如果没有任何Plan，Wait会直接返回成功
	ret, err := planBlder.Execute().Wait(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("executing io plan: %w", err)
	}

	if !shouldUpdateBlocks {
		return nil, nil
	}

	for k, v := range ret {
		idx, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing result key %s as index: %w", k, err)
		}

		newBlocks[idx].FileHash = v.(string)
	}

	return &coormq.UpdatingObjectRedundancy{
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

	_, err = agtCli.PinObject(agtmq.ReqPinObject([]string{fileHash}, false))
	if err != nil {
		return fmt.Errorf("start pinning object: %w", err)
	}

	return nil
}

func init() {
	RegisterMessageConvertor(NewCheckPackageRedundancy)
}
