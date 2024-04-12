package event

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/bitmap"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/lo2"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/common/utils/sort2"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/plans"
	agtmq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/agent"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
	scevt "gitlink.org.cn/cloudream/storage/common/pkgs/mq/scanner/event"
)

type CleanPinned struct {
	*scevt.CleanPinned
}

func NewCleanPinned(evt *scevt.CleanPinned) *CleanPinned {
	return &CleanPinned{
		CleanPinned: evt,
	}
}

func (t *CleanPinned) TryMerge(other Event) bool {
	event, ok := other.(*CleanPinned)
	if !ok {
		return false
	}

	return t.PackageID == event.PackageID
}

func (t *CleanPinned) Execute(execCtx ExecuteContext) {
	log := logger.WithType[CleanPinned]("Event")
	startTime := time.Now()
	log.Debugf("begin with %v", logger.FormatStruct(t.CleanPinned))
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

	getLoadLog, err := coorCli.GetPackageLoadLogDetails(coormq.ReqGetPackageLoadLogDetails(t.PackageID))
	if err != nil {
		log.Warnf("getting package load log details: %s", err.Error())
		return
	}
	readerNodeIDs := lo.Map(getLoadLog.Logs, func(item coormq.PackageLoadLogDetail, idx int) cdssdk.NodeID { return item.Storage.NodeID })

	// 注意！需要保证allNodeID包含所有之后可能用到的节点ID
	// TOOD 可以考虑设计Cache机制
	var allNodeID []cdssdk.NodeID
	for _, obj := range getObjs.Objects {
		for _, block := range obj.Blocks {
			allNodeID = append(allNodeID, block.NodeID)
		}
		allNodeID = append(allNodeID, obj.PinnedAt...)
	}
	allNodeID = append(allNodeID, readerNodeIDs...)

	getNodeResp, err := coorCli.GetNodes(coormq.NewGetNodes(lo.Union(allNodeID)))
	if err != nil {
		log.Warnf("getting nodes: %s", err.Error())
		return
	}

	allNodeInfos := make(map[cdssdk.NodeID]*cdssdk.Node)
	for _, node := range getNodeResp.Nodes {
		n := node
		allNodeInfos[node.NodeID] = &n
	}

	// 只对ec和rep对象进行处理
	var ecObjects []stgmod.ObjectDetail
	var repObjects []stgmod.ObjectDetail
	for _, obj := range getObjs.Objects {
		if _, ok := obj.Object.Redundancy.(*cdssdk.ECRedundancy); ok {
			ecObjects = append(ecObjects, obj)
		} else if _, ok := obj.Object.Redundancy.(*cdssdk.RepRedundancy); ok {
			repObjects = append(repObjects, obj)
		}
	}

	planBld := plans.NewPlanBuilder()
	pinPlans := make(map[cdssdk.NodeID]*[]string)

	// 对于rep对象，统计出所有对象块分布最多的两个节点，用这两个节点代表所有rep对象块的分布，去进行退火算法
	var repObjectsUpdating []coormq.UpdatingObjectRedundancy
	repMostNodeIDs := t.summaryRepObjectBlockNodes(repObjects)
	solu := t.startAnnealing(allNodeInfos, readerNodeIDs, annealingObject{
		totalBlockCount: 1,
		minBlockCnt:     1,
		pinnedAt:        repMostNodeIDs,
		blocks:          nil,
	})
	for _, obj := range repObjects {
		repObjectsUpdating = append(repObjectsUpdating, t.makePlansForRepObject(solu, obj, pinPlans))
	}

	// 对于ec对象，则每个对象单独进行退火算法
	var ecObjectsUpdating []coormq.UpdatingObjectRedundancy
	for _, obj := range ecObjects {
		ecRed := obj.Object.Redundancy.(*cdssdk.ECRedundancy)
		solu := t.startAnnealing(allNodeInfos, readerNodeIDs, annealingObject{
			totalBlockCount: ecRed.N,
			minBlockCnt:     ecRed.K,
			pinnedAt:        obj.PinnedAt,
			blocks:          obj.Blocks,
		})
		ecObjectsUpdating = append(ecObjectsUpdating, t.makePlansForECObject(allNodeInfos, solu, obj, &planBld))
	}

	ioSwRets, err := t.executePlans(execCtx, pinPlans, &planBld)
	if err != nil {
		log.Warn(err.Error())
		return
	}

	// 根据按照方案进行调整的结果，填充更新元数据的命令
	for i := range ecObjectsUpdating {
		t.populateECObjectEntry(&ecObjectsUpdating[i], ecObjects[i], ioSwRets)
	}

	finalEntries := append(repObjectsUpdating, ecObjectsUpdating...)
	if len(finalEntries) > 0 {
		_, err = coorCli.UpdateObjectRedundancy(coormq.ReqUpdateObjectRedundancy(finalEntries))
		if err != nil {
			log.Warnf("changing object redundancy: %s", err.Error())
			return
		}
	}
}

func (t *CleanPinned) summaryRepObjectBlockNodes(objs []stgmod.ObjectDetail) []cdssdk.NodeID {
	type nodeBlocks struct {
		NodeID cdssdk.NodeID
		Count  int
	}

	nodeBlocksMap := make(map[cdssdk.NodeID]*nodeBlocks)
	for _, obj := range objs {
		cacheBlockNodes := make(map[cdssdk.NodeID]bool)
		for _, block := range obj.Blocks {
			if _, ok := nodeBlocksMap[block.NodeID]; !ok {
				nodeBlocksMap[block.NodeID] = &nodeBlocks{
					NodeID: block.NodeID,
					Count:  0,
				}
			}
			nodeBlocksMap[block.NodeID].Count++
			cacheBlockNodes[block.NodeID] = true
		}

		for _, nodeID := range obj.PinnedAt {
			if cacheBlockNodes[nodeID] {
				continue
			}

			if _, ok := nodeBlocksMap[nodeID]; !ok {
				nodeBlocksMap[nodeID] = &nodeBlocks{
					NodeID: nodeID,
					Count:  0,
				}
			}
			nodeBlocksMap[nodeID].Count++
		}
	}

	nodes := lo.Values(nodeBlocksMap)
	sort2.Sort(nodes, func(left *nodeBlocks, right *nodeBlocks) int {
		return right.Count - left.Count
	})

	// 只选出块数超过一半的节点，但要保证至少有两个节点
	for i := 2; i < len(nodes); i++ {
		if nodes[i].Count < len(objs)/2 {
			nodes = nodes[:i]
			break
		}
	}

	return lo.Map(nodes, func(item *nodeBlocks, idx int) cdssdk.NodeID { return item.NodeID })
}

type annealingState struct {
	allNodeInfos        map[cdssdk.NodeID]*cdssdk.Node     // 所有节点的信息
	readerNodeIDs       []cdssdk.NodeID                    // 近期可能访问此对象的节点
	nodesSortedByReader map[cdssdk.NodeID][]nodeDist       // 拥有数据的节点到每个可能访问对象的节点按距离排序
	object              annealingObject                    // 进行退火的对象
	blockList           []objectBlock                      // 排序后的块分布情况
	nodeBlockBitmaps    map[cdssdk.NodeID]*bitmap.Bitmap64 // 用位图的形式表示每一个节点上有哪些块
	nodeCombTree        combinatorialTree                  // 节点组合树，用于加速计算容灾度

	maxScore         float64 // 搜索过程中得到过的最大分数
	maxScoreRmBlocks []bool  // 最大分数对应的删除方案

	rmBlocks      []bool  // 当前删除方案
	inversedIndex int     // 当前删除方案是从上一次的方案改动哪个flag而来的
	lastScore     float64 // 上一次方案的分数
}

type objectBlock struct {
	Index     int
	NodeID    cdssdk.NodeID
	HasEntity bool   // 节点拥有实际的文件数据块
	HasShadow bool   // 如果节点拥有完整文件数据，那么认为这个节点拥有所有块，这些块被称为影子块
	FileHash  string // 只有在拥有实际文件数据块时，这个字段才有值
}

type nodeDist struct {
	NodeID   cdssdk.NodeID
	Distance float64
}

type combinatorialTree struct {
	nodes               []combinatorialTreeNode
	blocksMaps          map[int]bitmap.Bitmap64
	nodeIDToLocalNodeID map[cdssdk.NodeID]int
	localNodeIDToNodeID []cdssdk.NodeID
}

type annealingObject struct {
	totalBlockCount int
	minBlockCnt     int
	pinnedAt        []cdssdk.NodeID
	blocks          []stgmod.ObjectBlock
}

const (
	iterActionNone  = 0
	iterActionSkip  = 1
	iterActionBreak = 2
)

func newCombinatorialTree(nodeBlocksMaps map[cdssdk.NodeID]*bitmap.Bitmap64) combinatorialTree {
	tree := combinatorialTree{
		blocksMaps:          make(map[int]bitmap.Bitmap64),
		nodeIDToLocalNodeID: make(map[cdssdk.NodeID]int),
	}

	tree.nodes = make([]combinatorialTreeNode, (1 << len(nodeBlocksMaps)))
	for id, mp := range nodeBlocksMaps {
		tree.nodeIDToLocalNodeID[id] = len(tree.localNodeIDToNodeID)
		tree.blocksMaps[len(tree.localNodeIDToNodeID)] = *mp
		tree.localNodeIDToNodeID = append(tree.localNodeIDToNodeID, id)
	}

	tree.nodes[0].localNodeID = -1
	index := 1
	tree.initNode(0, &tree.nodes[0], &index)

	return tree
}

func (t *combinatorialTree) initNode(minAvaiLocalNodeID int, parent *combinatorialTreeNode, index *int) {
	for i := minAvaiLocalNodeID; i < len(t.nodeIDToLocalNodeID); i++ {
		curIndex := *index
		*index++
		bitMp := t.blocksMaps[i]
		bitMp.Or(&parent.blocksBitmap)

		t.nodes[curIndex] = combinatorialTreeNode{
			localNodeID:  i,
			parent:       parent,
			blocksBitmap: bitMp,
		}
		t.initNode(i+1, &t.nodes[curIndex], index)
	}
}

// 获得索引指定的节点所在的层
func (t *combinatorialTree) GetDepth(index int) int {
	depth := 0

	// 反复判断节点在哪个子树。从左到右，子树节点的数量呈现8 4 2的变化，由此可以得到每个子树的索引值的范围
	subTreeCount := 1 << len(t.nodeIDToLocalNodeID)
	for index > 0 {
		if index < subTreeCount {
			// 定位到一个子树后，深度+1，然后进入这个子树，使用同样的方法再进行定位。
			// 进入子树后需要将索引值-1，因为要去掉子树的根节点
			index--
			depth++
		} else {
			// 如果索引值不在这个子树范围内，则将值减去子树的节点数量，
			// 这样每一次都可以视为使用同样的逻辑对不同大小的树进行判断。
			index -= subTreeCount
		}
		subTreeCount >>= 1
	}

	return depth
}

// 更新某一个算力中心节点的块分布位图，同时更新它对应组合树节点的所有子节点。
// 如果更新到某个节点时，已有K个块，那么就不会再更新它的子节点
func (t *combinatorialTree) UpdateBitmap(nodeID cdssdk.NodeID, mp bitmap.Bitmap64, k int) {
	t.blocksMaps[t.nodeIDToLocalNodeID[nodeID]] = mp
	// 首先定义两种遍历树节点时的移动方式：
	//  1. 竖直移动（深度增加）：从一个节点移动到它最左边的子节点。每移动一步，index+1
	//  2. 水平移动：从一个节点移动到它右边的兄弟节点。每移动一步，根据它所在的深度，index+8，+4，+2
	// LocalNodeID从0开始，将其+1后得到移动步数steps。
	// 将移动步数拆成多部分，分配到上述的两种移动方式上，并进行任意组合，且保证第一次为至少进行一次的竖直移动，移动之后的节点都会是同一个计算中心节点。
	steps := t.nodeIDToLocalNodeID[nodeID] + 1
	for d := 1; d <= steps; d++ {
		t.iterCombBits(len(t.nodeIDToLocalNodeID)-1, steps-d, 0, func(i int) {
			index := d + i
			node := &t.nodes[index]

			newMp := t.blocksMaps[node.localNodeID]
			newMp.Or(&node.parent.blocksBitmap)
			node.blocksBitmap = newMp
			if newMp.Weight() >= k {
				return
			}

			t.iterChildren(index, func(index, parentIndex, depth int) int {
				curNode := &t.nodes[index]
				parentNode := t.nodes[parentIndex]

				newMp := t.blocksMaps[curNode.localNodeID]
				newMp.Or(&parentNode.blocksBitmap)
				curNode.blocksBitmap = newMp
				if newMp.Weight() >= k {
					return iterActionSkip
				}

				return iterActionNone
			})
		})
	}
}

// 遍历树，找到至少拥有K个块的树节点的最大深度
func (t *combinatorialTree) FindKBlocksMaxDepth(k int) int {
	maxDepth := -1
	t.iterChildren(0, func(index, parentIndex, depth int) int {
		if t.nodes[index].blocksBitmap.Weight() >= k {
			if maxDepth < depth {
				maxDepth = depth
			}
			return iterActionSkip
		}
		// 如果到了叶子节点，还没有找到K个块，那就认为要满足K个块，至少需要再多一个节点，即深度+1。
		// 由于遍历时采用的是深度优先的算法，因此遍历到这个叶子节点时，叶子节点再加一个节点的组合已经在前面搜索过，
		// 所以用当前叶子节点深度+1来作为当前分支的结果就可以，即使当前情况下增加任意一个节点依然不够K块，
		// 可以使用同样的思路去递推到当前叶子节点增加两个块的情况。
		if t.nodes[index].localNodeID == len(t.nodeIDToLocalNodeID)-1 {
			if maxDepth < depth+1 {
				maxDepth = depth + 1
			}
		}

		return iterActionNone
	})

	if maxDepth == -1 || maxDepth > len(t.nodeIDToLocalNodeID) {
		return len(t.nodeIDToLocalNodeID)
	}

	return maxDepth
}

func (t *combinatorialTree) iterCombBits(width int, count int, offset int, callback func(int)) {
	if count == 0 {
		callback(offset)
		return
	}

	for b := width; b >= count; b-- {
		t.iterCombBits(b-1, count-1, offset+(1<<b), callback)
	}
}

func (t *combinatorialTree) iterChildren(index int, do func(index int, parentIndex int, depth int) int) {
	curNode := &t.nodes[index]
	childIndex := index + 1
	curDepth := t.GetDepth(index)

	childCounts := len(t.nodeIDToLocalNodeID) - 1 - curNode.localNodeID
	if childCounts == 0 {
		return
	}

	childTreeNodeCnt := 1 << (childCounts - 1)
	for c := 0; c < childCounts; c++ {
		act := t.itering(childIndex, index, curDepth+1, do)
		if act == iterActionBreak {
			return
		}

		childIndex += childTreeNodeCnt
		childTreeNodeCnt >>= 1
	}
}

func (t *combinatorialTree) itering(index int, parentIndex int, depth int, do func(index int, parentIndex int, depth int) int) int {
	act := do(index, parentIndex, depth)
	if act == iterActionBreak {
		return act
	}
	if act == iterActionSkip {
		return iterActionNone
	}

	curNode := &t.nodes[index]
	childIndex := index + 1

	childCounts := len(t.nodeIDToLocalNodeID) - 1 - curNode.localNodeID
	if childCounts == 0 {
		return iterActionNone
	}

	childTreeNodeCnt := 1 << (childCounts - 1)
	for c := 0; c < childCounts; c++ {
		act = t.itering(childIndex, index, depth+1, do)
		if act == iterActionBreak {
			return act
		}

		childIndex += childTreeNodeCnt
		childTreeNodeCnt >>= 1
	}

	return iterActionNone
}

type combinatorialTreeNode struct {
	localNodeID  int
	parent       *combinatorialTreeNode
	blocksBitmap bitmap.Bitmap64 // 选择了这个中心之后，所有中心一共包含多少种块
}

type annealingSolution struct {
	blockList []objectBlock // 所有节点的块分布情况
	rmBlocks  []bool        // 要删除哪些块
}

func (t *CleanPinned) startAnnealing(allNodeInfos map[cdssdk.NodeID]*cdssdk.Node, readerNodeIDs []cdssdk.NodeID, object annealingObject) annealingSolution {
	state := &annealingState{
		allNodeInfos:        allNodeInfos,
		readerNodeIDs:       readerNodeIDs,
		nodesSortedByReader: make(map[cdssdk.NodeID][]nodeDist),
		object:              object,
		nodeBlockBitmaps:    make(map[cdssdk.NodeID]*bitmap.Bitmap64),
	}

	t.initBlockList(state)
	if state.blockList == nil {
		return annealingSolution{}
	}

	t.initNodeBlockBitmap(state)

	t.sortNodeByReaderDistance(state)

	state.rmBlocks = make([]bool, len(state.blockList))
	state.inversedIndex = -1
	state.nodeCombTree = newCombinatorialTree(state.nodeBlockBitmaps)

	state.lastScore = t.calcScore(state)
	state.maxScore = state.lastScore
	state.maxScoreRmBlocks = lo2.ArrayClone(state.rmBlocks)

	// 模拟退火算法的温度
	curTemp := state.lastScore
	// 结束温度
	finalTemp := curTemp * 0.2
	// 冷却率
	coolingRate := 0.95

	for curTemp > finalTemp {
		state.inversedIndex = rand.Intn(len(state.rmBlocks))
		block := state.blockList[state.inversedIndex]
		state.rmBlocks[state.inversedIndex] = !state.rmBlocks[state.inversedIndex]
		state.nodeBlockBitmaps[block.NodeID].Set(block.Index, !state.rmBlocks[state.inversedIndex])
		state.nodeCombTree.UpdateBitmap(block.NodeID, *state.nodeBlockBitmaps[block.NodeID], state.object.minBlockCnt)

		curScore := t.calcScore(state)

		dScore := curScore - state.lastScore
		// 如果新方案比旧方案得分低，且没有要求强制接受新方案，那么就将变化改回去
		if curScore == 0 || (dScore < 0 && !t.alwaysAccept(curTemp, dScore, coolingRate)) {
			state.rmBlocks[state.inversedIndex] = !state.rmBlocks[state.inversedIndex]
			state.nodeBlockBitmaps[block.NodeID].Set(block.Index, !state.rmBlocks[state.inversedIndex])
			state.nodeCombTree.UpdateBitmap(block.NodeID, *state.nodeBlockBitmaps[block.NodeID], state.object.minBlockCnt)
			// fmt.Printf("\n")
		} else {
			// fmt.Printf(" accept!\n")
			state.lastScore = curScore
			if state.maxScore < curScore {
				state.maxScore = state.lastScore
				state.maxScoreRmBlocks = lo2.ArrayClone(state.rmBlocks)
			}
		}
		curTemp *= coolingRate
	}
	// fmt.Printf("final: %v\n", state.maxScoreRmBlocks)
	return annealingSolution{
		blockList: state.blockList,
		rmBlocks:  state.maxScoreRmBlocks,
	}
}

func (t *CleanPinned) initBlockList(ctx *annealingState) {
	blocksMap := make(map[cdssdk.NodeID][]objectBlock)

	// 先生成所有的影子块
	for _, pinned := range ctx.object.pinnedAt {
		blocks := make([]objectBlock, 0, ctx.object.totalBlockCount)
		for i := 0; i < ctx.object.totalBlockCount; i++ {
			blocks = append(blocks, objectBlock{
				Index:     i,
				NodeID:    pinned,
				HasShadow: true,
			})
		}
		blocksMap[pinned] = blocks
	}

	// 再填充实际块
	for _, b := range ctx.object.blocks {
		blocks := blocksMap[b.NodeID]

		has := false
		for i := range blocks {
			if blocks[i].Index == b.Index {
				blocks[i].HasEntity = true
				blocks[i].FileHash = b.FileHash
				has = true
				break
			}
		}

		if has {
			continue
		}

		blocks = append(blocks, objectBlock{
			Index:     b.Index,
			NodeID:    b.NodeID,
			HasEntity: true,
			FileHash:  b.FileHash,
		})
		blocksMap[b.NodeID] = blocks
	}

	var sortedBlocks []objectBlock
	for _, bs := range blocksMap {
		sortedBlocks = append(sortedBlocks, bs...)
	}
	sortedBlocks = sort2.Sort(sortedBlocks, func(left objectBlock, right objectBlock) int {
		d := left.NodeID - right.NodeID
		if d != 0 {
			return int(d)
		}

		return left.Index - right.Index
	})

	ctx.blockList = sortedBlocks
}

func (t *CleanPinned) initNodeBlockBitmap(state *annealingState) {
	for _, b := range state.blockList {
		mp, ok := state.nodeBlockBitmaps[b.NodeID]
		if !ok {
			nb := bitmap.Bitmap64(0)
			mp = &nb
			state.nodeBlockBitmaps[b.NodeID] = mp
		}
		mp.Set(b.Index, true)
	}
}

func (t *CleanPinned) sortNodeByReaderDistance(state *annealingState) {
	for _, r := range state.readerNodeIDs {
		var nodeDists []nodeDist

		for n := range state.nodeBlockBitmaps {
			if r == n {
				// 同节点时距离视为0.1
				nodeDists = append(nodeDists, nodeDist{
					NodeID:   n,
					Distance: consts.NodeDistanceSameNode,
				})
			} else if state.allNodeInfos[r].LocationID == state.allNodeInfos[n].LocationID {
				// 同地区时距离视为1
				nodeDists = append(nodeDists, nodeDist{
					NodeID:   n,
					Distance: consts.NodeDistanceSameLocation,
				})
			} else {
				// 不同地区时距离视为5
				nodeDists = append(nodeDists, nodeDist{
					NodeID:   n,
					Distance: consts.NodeDistanceOther,
				})
			}
		}

		state.nodesSortedByReader[r] = sort2.Sort(nodeDists, func(left, right nodeDist) int { return sort2.Cmp(left.Distance, right.Distance) })
	}
}

func (t *CleanPinned) calcScore(state *annealingState) float64 {
	dt := t.calcDisasterTolerance(state)
	ac := t.calcMinAccessCost(state)
	sc := t.calcSpaceCost(state)

	dtSc := 1.0
	if dt < 1 {
		dtSc = 0
	} else if dt >= 2 {
		dtSc = 1.5
	}

	newSc := 0.0
	if dt == 0 || ac == 0 {
		newSc = 0
	} else {
		newSc = dtSc / (sc * ac)
	}

	// fmt.Printf("solu: %v, cur: %v, dt: %v, ac: %v, sc: %v \n", state.rmBlocks, newSc, dt, ac, sc)
	return newSc
}

// 计算容灾度
func (t *CleanPinned) calcDisasterTolerance(state *annealingState) float64 {
	if state.inversedIndex != -1 {
		node := state.blockList[state.inversedIndex]
		state.nodeCombTree.UpdateBitmap(node.NodeID, *state.nodeBlockBitmaps[node.NodeID], state.object.minBlockCnt)
	}
	return float64(len(state.nodeBlockBitmaps) - state.nodeCombTree.FindKBlocksMaxDepth(state.object.minBlockCnt))
}

// 计算最小访问数据的代价
func (t *CleanPinned) calcMinAccessCost(state *annealingState) float64 {
	cost := math.MaxFloat64
	for _, reader := range state.readerNodeIDs {
		tarNodes := state.nodesSortedByReader[reader]
		gotBlocks := bitmap.Bitmap64(0)
		thisCost := 0.0

		for _, tar := range tarNodes {
			tarNodeMp := state.nodeBlockBitmaps[tar.NodeID]

			// 只需要从目的节点上获得缺少的块
			curWeigth := gotBlocks.Weight()
			// 下面的if会在拿到k个块之后跳出循环，所以or多了块也没关系
			gotBlocks.Or(tarNodeMp)
			// 但是算读取块的消耗时，不能多算，最多算读了k个块的消耗
			willGetBlocks := math2.Min(gotBlocks.Weight()-curWeigth, state.object.minBlockCnt-curWeigth)
			thisCost += float64(willGetBlocks) * float64(tar.Distance)

			if gotBlocks.Weight() >= state.object.minBlockCnt {
				break
			}
		}
		if gotBlocks.Weight() >= state.object.minBlockCnt {
			cost = math.Min(cost, thisCost)
		}
	}

	return cost
}

// 计算冗余度
func (t *CleanPinned) calcSpaceCost(ctx *annealingState) float64 {
	blockCount := 0
	for i, b := range ctx.blockList {
		if ctx.rmBlocks[i] {
			continue
		}

		if b.HasEntity {
			blockCount++
		}
		if b.HasShadow {
			blockCount++
		}
	}
	// 所有算力中心上拥有的块的总数 / 一个对象被分成了几个块
	return float64(blockCount) / float64(ctx.object.minBlockCnt)
}

// 如果新方案得分比旧方案小，那么在一定概率内也接受新方案
func (t *CleanPinned) alwaysAccept(curTemp float64, dScore float64, coolingRate float64) bool {
	v := math.Exp(dScore / curTemp / coolingRate)
	// fmt.Printf(" -- chance: %v, temp: %v", v, curTemp)
	return v > rand.Float64()
}

func (t *CleanPinned) makePlansForRepObject(solu annealingSolution, obj stgmod.ObjectDetail, pinPlans map[cdssdk.NodeID]*[]string) coormq.UpdatingObjectRedundancy {
	entry := coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: obj.Object.Redundancy,
	}

	for i, f := range solu.rmBlocks {
		hasCache := lo.ContainsBy(obj.Blocks, func(b stgmod.ObjectBlock) bool { return b.NodeID == solu.blockList[i].NodeID }) ||
			lo.ContainsBy(obj.PinnedAt, func(n cdssdk.NodeID) bool { return n == solu.blockList[i].NodeID })
		willRm := f

		if !willRm {
			// 如果对象在退火后要保留副本的节点没有副本，则需要在这个节点创建副本
			if !hasCache {
				pinPlan, ok := pinPlans[solu.blockList[i].NodeID]
				if !ok {
					pinPlan = &[]string{}
					pinPlans[solu.blockList[i].NodeID] = pinPlan
				}
				*pinPlan = append(*pinPlan, obj.Object.FileHash)
			}
			entry.Blocks = append(entry.Blocks, stgmod.ObjectBlock{
				ObjectID: obj.Object.ObjectID,
				Index:    solu.blockList[i].Index,
				NodeID:   solu.blockList[i].NodeID,
				FileHash: obj.Object.FileHash,
			})
		}
	}

	return entry
}

func (t *CleanPinned) makePlansForECObject(allNodeInfos map[cdssdk.NodeID]*cdssdk.Node, solu annealingSolution, obj stgmod.ObjectDetail, planBld *plans.PlanBuilder) coormq.UpdatingObjectRedundancy {
	entry := coormq.UpdatingObjectRedundancy{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: obj.Object.Redundancy,
	}

	reconstrct := make(map[cdssdk.NodeID]*[]int)
	for i, f := range solu.rmBlocks {
		block := solu.blockList[i]
		if !f {
			entry.Blocks = append(entry.Blocks, stgmod.ObjectBlock{
				ObjectID: obj.Object.ObjectID,
				Index:    block.Index,
				NodeID:   block.NodeID,
				FileHash: block.FileHash,
			})

			// 如果这个块是影子块，那么就要从完整对象里重建这个块
			if !block.HasEntity {
				re, ok := reconstrct[block.NodeID]
				if !ok {
					re = &[]int{}
					reconstrct[block.NodeID] = re
				}

				*re = append(*re, block.Index)
			}
		}
	}

	ecRed := obj.Object.Redundancy.(*cdssdk.ECRedundancy)

	for id, idxs := range reconstrct {
		agt := planBld.AtAgent(*allNodeInfos[id])

		strs := agt.IPFSRead(obj.Object.FileHash).ChunkedSplit(ecRed.ChunkSize, ecRed.K, true)
		ss := agt.ECReconstructAny(*ecRed, lo.Range(ecRed.K), *idxs, strs.Streams...)
		for i, s := range ss.Streams {
			s.IPFSWrite(fmt.Sprintf("%d.%d", obj.Object.ObjectID, (*idxs)[i]))
		}
	}
	return entry
}

func (t *CleanPinned) executePlans(execCtx ExecuteContext, pinPlans map[cdssdk.NodeID]*[]string, planBld *plans.PlanBuilder) (map[string]any, error) {
	log := logger.WithType[CleanPinned]("Event")

	ioPlan, err := planBld.Build()
	if err != nil {
		return nil, fmt.Errorf("building io switch plan: %w", err)
	}

	// 统一加锁，有重复也没关系
	lockBld := reqbuilder.NewBuilder()
	for nodeID := range pinPlans {
		lockBld.IPFS().Buzy(nodeID)
	}
	for _, plan := range ioPlan.AgentPlans {
		lockBld.IPFS().Buzy(plan.Node.NodeID)
	}
	lock, err := lockBld.MutexLock(execCtx.Args.DistLock)
	if err != nil {
		return nil, fmt.Errorf("acquiring distlock: %w", err)
	}
	defer lock.Unlock()

	wg := sync.WaitGroup{}

	// 执行pin操作
	var anyPinErr error
	for nodeID, pin := range pinPlans {
		wg.Add(1)
		go func(nodeID cdssdk.NodeID, pin *[]string) {
			defer wg.Done()

			agtCli, err := stgglb.AgentMQPool.Acquire(nodeID)
			if err != nil {
				log.Warnf("new agent client: %s", err.Error())
				return
			}
			defer stgglb.AgentMQPool.Release(agtCli)

			_, err = agtCli.PinObject(agtmq.ReqPinObject(*pin, false))
			if err != nil {
				log.Warnf("pinning object: %s", err.Error())
				anyPinErr = err
			}
		}(nodeID, pin)
	}

	// 执行IO计划
	var ioSwRets map[string]any
	var ioSwErr error
	wg.Add(1)
	go func() {
		defer wg.Done()

		exec, err := plans.Execute(*ioPlan)
		if err != nil {
			ioSwErr = fmt.Errorf("executing io switch plan: %w", err)
			return
		}
		ret, err := exec.Wait()
		if err != nil {
			ioSwErr = fmt.Errorf("waiting io switch plan: %w", err)
			return
		}
		ioSwRets = ret.ResultValues
	}()

	wg.Wait()

	if anyPinErr != nil {
		return nil, anyPinErr
	}

	if ioSwErr != nil {
		return nil, ioSwErr
	}

	return ioSwRets, nil
}

func (t *CleanPinned) populateECObjectEntry(entry *coormq.UpdatingObjectRedundancy, obj stgmod.ObjectDetail, ioRets map[string]any) {
	for i := range entry.Blocks {
		if entry.Blocks[i].FileHash != "" {
			continue
		}

		key := fmt.Sprintf("%d.%d", obj.Object.ObjectID, entry.Blocks[i].Index)
		// 不应该出现key不存在的情况
		entry.Blocks[i].FileHash = ioRets[key].(string)
	}
}

func init() {
	RegisterMessageConvertor(NewCleanPinned)
}
