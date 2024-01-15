package event

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"

	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/pkgs/bitmap"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	mylo "gitlink.org.cn/cloudream/common/utils/lo"
	mymath "gitlink.org.cn/cloudream/common/utils/math"
	myref "gitlink.org.cn/cloudream/common/utils/reflect"
	mysort "gitlink.org.cn/cloudream/common/utils/sort"
	"gitlink.org.cn/cloudream/storage/common/consts"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/distlock/reqbuilder"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/plans"
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
	log.Debugf("begin with %v", logger.FormatStruct(t.CleanPinned))
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

	getLoadLog, err := coorCli.GetPackageLoadLogDetails(coormq.ReqGetPackageLoadLogDetails(t.PackageID))
	if err != nil {
		log.Warnf("getting package load log details: %s", err.Error())
		return
	}
	readerNodeIDs := lo.Map(getLoadLog.Logs, func(item coormq.PackageLoadLogDetail, idx int) cdssdk.NodeID { return item.Storage.NodeID })

	var changeRedEntries []coormq.ChangeObjectRedundancyEntry
	for _, obj := range getObjs.Objects {
		entry, err := t.doOne(execCtx, readerNodeIDs, coorCli, obj)
		if err != nil {
			log.WithField("PackageID", obj).Warn(err.Error())
			continue
		}
		if entry != nil {
			changeRedEntries = append(changeRedEntries, *entry)
		}
	}

	if len(changeRedEntries) > 0 {
		_, err = coorCli.ChangeObjectRedundancy(coormq.ReqChangeObjectRedundancy(changeRedEntries))
		if err != nil {
			log.Warnf("changing object redundancy: %s", err.Error())
			return
		}
	}
}

type doingContext struct {
	execCtx             ExecuteContext
	readerNodeIDs       []cdssdk.NodeID              // 近期可能访问此对象的节点
	nodesSortedByReader map[cdssdk.NodeID][]nodeDist // 拥有数据的节点到每个可能访问对象的节点按距离排序
	nodeInfos           map[cdssdk.NodeID]*model.Node
	blockList           []objectBlock                      // 排序后的块分布情况
	nodeBlockBitmaps    map[cdssdk.NodeID]*bitmap.Bitmap64 // 用位图的形式表示每一个节点上有哪些块
	allBlockTypeCount   int                                // object总共被分成了几块
	minBlockTypeCount   int                                // 最少要几块才能恢复出完整的object
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

func (t *CleanPinned) doOne(execCtx ExecuteContext, readerNodeIDs []cdssdk.NodeID, coorCli *coormq.Client, obj stgmod.ObjectDetail) (*coormq.ChangeObjectRedundancyEntry, error) {
	if len(obj.PinnedAt) == 0 && len(obj.Blocks) == 0 {
		return nil, nil
	}

	ctx := doingContext{
		execCtx:             execCtx,
		readerNodeIDs:       readerNodeIDs,
		nodesSortedByReader: make(map[cdssdk.NodeID][]nodeDist),
		nodeInfos:           make(map[cdssdk.NodeID]*model.Node),
		nodeBlockBitmaps:    make(map[cdssdk.NodeID]*bitmap.Bitmap64),
	}

	err := t.getNodeInfos(&ctx, coorCli, obj)
	if err != nil {
		return nil, err
	}

	err = t.makeBlockList(&ctx, obj)
	if err != nil {
		return nil, err
	}

	if ctx.blockList == nil {
		return nil, nil
	}

	t.makeNodeBlockBitmap(&ctx)

	t.sortNodeByReaderDistance(&ctx)

	ctx.rmBlocks = make([]bool, len(ctx.blockList))
	ctx.inversedIndex = -1
	ctx.nodeCombTree = newCombinatorialTree(ctx.nodeBlockBitmaps)

	ctx.lastScore = t.calcScore(&ctx)
	ctx.maxScore = ctx.lastScore
	ctx.maxScoreRmBlocks = mylo.ArrayClone(ctx.rmBlocks)

	// 模拟退火算法的温度
	curTemp := ctx.lastScore
	// 结束温度
	finalTemp := curTemp * 0.2
	// 冷却率
	coolingRate := 0.95

	for curTemp > finalTemp {
		ctx.inversedIndex = rand.Intn(len(ctx.rmBlocks))
		block := ctx.blockList[ctx.inversedIndex]
		ctx.rmBlocks[ctx.inversedIndex] = !ctx.rmBlocks[ctx.inversedIndex]
		ctx.nodeBlockBitmaps[block.NodeID].Set(block.Index, !ctx.rmBlocks[ctx.inversedIndex])
		ctx.nodeCombTree.UpdateBitmap(block.NodeID, *ctx.nodeBlockBitmaps[block.NodeID], ctx.minBlockTypeCount)

		curScore := t.calcScore(&ctx)

		dScore := curScore - ctx.lastScore
		// 如果新方案比旧方案得分低，且没有要求强制接受新方案，那么就将变化改回去
		if curScore == 0 || (dScore < 0 && !t.alwaysAccept(curTemp, dScore, coolingRate)) {
			ctx.rmBlocks[ctx.inversedIndex] = !ctx.rmBlocks[ctx.inversedIndex]
			ctx.nodeBlockBitmaps[block.NodeID].Set(block.Index, !ctx.rmBlocks[ctx.inversedIndex])
			ctx.nodeCombTree.UpdateBitmap(block.NodeID, *ctx.nodeBlockBitmaps[block.NodeID], ctx.minBlockTypeCount)
			fmt.Printf("\n")
		} else {
			fmt.Printf(" accept!\n")
			ctx.lastScore = curScore
			if ctx.maxScore < curScore {
				ctx.maxScore = ctx.lastScore
				ctx.maxScoreRmBlocks = mylo.ArrayClone(ctx.rmBlocks)
			}
		}
		curTemp *= coolingRate
	}

	return t.applySolution(ctx, obj)
}

func (t *CleanPinned) getNodeInfos(ctx *doingContext, coorCli *coormq.Client, obj stgmod.ObjectDetail) error {
	var nodeIDs []cdssdk.NodeID
	for _, b := range obj.Blocks {
		nodeIDs = append(nodeIDs, b.NodeID)
	}
	nodeIDs = append(nodeIDs, obj.PinnedAt...)

	nodeIDs = append(nodeIDs, ctx.readerNodeIDs...)

	getNode, err := coorCli.GetNodes(coormq.NewGetNodes(lo.Uniq(nodeIDs)))
	if err != nil {
		return fmt.Errorf("requesting to coordinator: %w", err)
	}

	for _, n := range getNode.Nodes {
		ctx.nodeInfos[n.NodeID] = &n
	}

	return nil
}

func (t *CleanPinned) makeBlockList(ctx *doingContext, obj stgmod.ObjectDetail) error {
	blockCnt := 1
	minBlockCnt := 1
	switch red := obj.Object.Redundancy.(type) {
	case *cdssdk.NoneRedundancy:
		return nil
	case *cdssdk.RepRedundancy:
		blockCnt = 1
		minBlockCnt = 1
	case *cdssdk.ECRedundancy:
		blockCnt = red.N
		minBlockCnt = red.K
	default:
		return fmt.Errorf("unknow redundancy type: %v", myref.TypeOfValue(obj.Object.Redundancy))
	}

	blocksMap := make(map[cdssdk.NodeID][]objectBlock)

	// 先生成所有的影子块
	for _, pinned := range obj.PinnedAt {
		blocks := make([]objectBlock, 0, blockCnt)
		for i := 0; i < blockCnt; i++ {
			blocks = append(blocks, objectBlock{
				Index:     i,
				NodeID:    pinned,
				HasShadow: true,
			})
		}
		blocksMap[pinned] = blocks
	}

	// 再填充实际块
	for _, b := range obj.Blocks {
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
	sortedBlocks = mysort.Sort(sortedBlocks, func(left objectBlock, right objectBlock) int {
		d := left.NodeID - right.NodeID
		if d != 0 {
			return int(d)
		}

		return left.Index - right.Index
	})

	ctx.allBlockTypeCount = blockCnt
	ctx.minBlockTypeCount = minBlockCnt
	ctx.blockList = sortedBlocks
	return nil
}

func (t *CleanPinned) makeNodeBlockBitmap(ctx *doingContext) {
	for _, b := range ctx.blockList {
		mp, ok := ctx.nodeBlockBitmaps[b.NodeID]
		if !ok {
			nb := bitmap.Bitmap64(0)
			mp = &nb
			ctx.nodeBlockBitmaps[b.NodeID] = mp
		}
		mp.Set(b.Index, true)
	}
}

func (t *CleanPinned) sortNodeByReaderDistance(ctx *doingContext) {
	for _, r := range ctx.readerNodeIDs {
		var nodeDists []nodeDist

		for n := range ctx.nodeBlockBitmaps {
			if r == n {
				// 同节点时距离视为0.1
				nodeDists = append(nodeDists, nodeDist{
					NodeID:   n,
					Distance: consts.NodeDistanceSameNode,
				})
			} else if ctx.nodeInfos[r].LocationID == ctx.nodeInfos[n].LocationID {
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

		ctx.nodesSortedByReader[r] = mysort.Sort(nodeDists, func(left, right nodeDist) int { return mysort.Cmp(left.Distance, right.Distance) })
	}
}

func (t *CleanPinned) calcScore(ctx *doingContext) float64 {
	dt := t.calcDisasterTolerance(ctx)
	ac := t.calcMinAccessCost(ctx)
	sc := t.calcSpaceCost(ctx)

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

	fmt.Printf("solu: %v, cur: %v, dt: %v, ac: %v, sc: %v ", ctx.rmBlocks, newSc, dt, ac, sc)
	return newSc
}

// 计算容灾度
func (t *CleanPinned) calcDisasterTolerance(ctx *doingContext) float64 {
	if ctx.inversedIndex != -1 {
		node := ctx.blockList[ctx.inversedIndex]
		ctx.nodeCombTree.UpdateBitmap(node.NodeID, *ctx.nodeBlockBitmaps[node.NodeID], ctx.minBlockTypeCount)
	}
	return float64(len(ctx.nodeBlockBitmaps) - ctx.nodeCombTree.FindKBlocksMaxDepth(ctx.minBlockTypeCount))
}

// 计算最小访问数据的代价
func (t *CleanPinned) calcMinAccessCost(ctx *doingContext) float64 {
	cost := math.MaxFloat64
	for _, reader := range ctx.readerNodeIDs {
		tarNodes := ctx.nodesSortedByReader[reader]
		gotBlocks := bitmap.Bitmap64(0)
		thisCost := 0.0

		for _, tar := range tarNodes {
			tarNodeMp := ctx.nodeBlockBitmaps[tar.NodeID]

			// 只需要从目的节点上获得缺少的块
			curWeigth := gotBlocks.Weight()
			// 下面的if会在拿到k个块之后跳出循环，所以or多了块也没关系
			gotBlocks.Or(tarNodeMp)
			willGetBlocks := mymath.Min(gotBlocks.Weight()-curWeigth, ctx.minBlockTypeCount-curWeigth)
			thisCost += float64(willGetBlocks) * float64(tar.Distance)

			if gotBlocks.Weight() >= ctx.minBlockTypeCount {
				break
			}
		}
		if gotBlocks.Weight() >= ctx.minBlockTypeCount {
			cost = math.Min(cost, thisCost)
		}
	}

	return cost
}

// 计算冗余度
func (t *CleanPinned) calcSpaceCost(ctx *doingContext) float64 {
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
	return float64(blockCount) / float64(ctx.minBlockTypeCount)
}

// 如果新方案得分比旧方案小，那么在一定概率内也接受新方案
func (t *CleanPinned) alwaysAccept(curTemp float64, dScore float64, coolingRate float64) bool {
	v := math.Exp(dScore / curTemp / coolingRate)
	fmt.Printf(" -- chance: %v, temp: %v", v, curTemp)
	return v > rand.Float64()
}

func (t *CleanPinned) applySolution(ctx doingContext, obj stgmod.ObjectDetail) (*coormq.ChangeObjectRedundancyEntry, error) {
	entry := coormq.ChangeObjectRedundancyEntry{
		ObjectID:   obj.Object.ObjectID,
		Redundancy: obj.Object.Redundancy,
	}
	fmt.Printf("final solu: %v, score: %v\n", ctx.maxScoreRmBlocks, ctx.maxScore)

	reconstrct := make(map[cdssdk.NodeID]*[]int)
	for i, f := range ctx.maxScoreRmBlocks {
		block := ctx.blockList[i]
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

	bld := reqbuilder.NewBuilder()
	for id := range reconstrct {
		bld.IPFS().Buzy(id)
	}

	mutex, err := bld.MutexLock(ctx.execCtx.Args.DistLock)
	if err != nil {
		return nil, fmt.Errorf("acquiring distlock: %w", err)
	}
	defer mutex.Unlock()

	if ecRed, ok := obj.Object.Redundancy.(*cdssdk.ECRedundancy); ok {
		for id, idxs := range reconstrct {
			bld := plans.NewPlanBuilder()
			agt := bld.AtAgent(*ctx.nodeInfos[id])

			strs := agt.IPFSRead(obj.Object.FileHash).ChunkedSplit(ecRed.ChunkSize, ecRed.K, true)
			ss := agt.ECReconstructAny(*ecRed, lo.Range(ecRed.K), *idxs, strs.Streams...)
			for i, s := range ss.Streams {
				s.IPFSWrite(fmt.Sprintf("%d", (*idxs)[i]))
			}

			plan, err := bld.Build()
			if err != nil {
				return nil, fmt.Errorf("building io switch plan: %w", err)
			}

			exec, err := plans.Execute(*plan)
			if err != nil {
				return nil, fmt.Errorf("executing io switch plan: %w", err)
			}
			ret, err := exec.Wait()
			if err != nil {
				return nil, fmt.Errorf("executing io switch plan: %w", err)
			}

			for k, v := range ret.ResultValues {
				idx, err := strconv.ParseInt(k, 10, 32)
				if err != nil {
					return nil, fmt.Errorf("parsing plan result: %w", err)
				}

				for i := range entry.Blocks {
					if entry.Blocks[i].NodeID == id && entry.Blocks[i].Index == int(idx) {
						entry.Blocks[i].FileHash = v.(string)
					}
				}
			}

		}
	} else if _, ok := obj.Object.Redundancy.(*cdssdk.RepRedundancy); ok {
		// rep模式不分块，所以每一个Block的FileHash就是完整文件的FileHash
		for i := range entry.Blocks {
			entry.Blocks[i].FileHash = obj.Object.FileHash
		}
	}

	return &entry, nil
}

func init() {
	RegisterMessageConvertor(NewCleanPinned)
}
