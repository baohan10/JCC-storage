package event

import (
	"database/sql"
	"fmt"
	"math"

	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"gitlink.org.cn/cloudream/common/consts"
	"gitlink.org.cn/cloudream/common/pkg/logger"
	mymath "gitlink.org.cn/cloudream/common/utils/math"
	mysort "gitlink.org.cn/cloudream/common/utils/sort"
	"gitlink.org.cn/cloudream/scanner/internal/config"

	"gitlink.org.cn/cloudream/db/model"
	mysql "gitlink.org.cn/cloudream/db/sql"
	scevt "gitlink.org.cn/cloudream/rabbitmq/message/scanner/event"
)

type CheckRepCount struct {
	scevt.CheckRepCount
}

func NewCheckRepCount(fileHashes []string) *CheckRepCount {
	return &CheckRepCount{
		CheckRepCount: scevt.NewCheckRepCount(fileHashes),
	}
}

func (t *CheckRepCount) TryMerge(other Event) bool {
	event, ok := other.(*CheckRepCount)
	if !ok {
		return false
	}

	t.FileHashes = lo.Union(t.FileHashes, event.FileHashes)
	return true
}

func (t *CheckRepCount) Execute(execCtx ExecuteContext) {
	log := logger.WithType[CheckRepCount]("Event")
	log.Debugf("begin with %v", logger.FormatStruct(t))

	updatedNodeAndHashes := make(map[int][]string)

	for _, fileHash := range t.FileHashes {
		updatedNodeIDs, err := t.checkOneRepCount(fileHash, execCtx)
		if err != nil {
			log.WithField("FileHash", fileHash).Warnf("check file rep count failed, err: %s", err.Error())
			continue
		}

		for _, id := range updatedNodeIDs {
			hashes := updatedNodeAndHashes[id]
			updatedNodeAndHashes[id] = append(hashes, fileHash)
		}
	}

	for nodeID, hashes := range updatedNodeAndHashes {
		// 新任务继承本任务的执行设定（紧急任务依然保持紧急任务）
		execCtx.Executor.Post(NewAgentCheckCache(nodeID, hashes), execCtx.Option)
	}
}

func (t *CheckRepCount) checkOneRepCount(fileHash string, execCtx ExecuteContext) ([]int, error) {
	log := logger.WithType[CheckRepCount]("Event")

	var updatedNodeIDs []int
	err := execCtx.Args.DB.DoTx(sql.LevelSerializable, func(tx *sqlx.Tx) error {
		repMaxCnt, err := mysql.ObjectRep.GetFileMaxRepCount(tx, fileHash)
		if err != nil {
			return fmt.Errorf("get file max rep count failed, err: %w", err)
		}

		blkCnt, err := mysql.ObjectBlock.CountBlockWithHash(tx, fileHash)
		if err != nil {
			return fmt.Errorf("count block with hash failed, err: %w", err)
		}

		// 计算所需的最少备份数：
		// ObjectRep中期望备份数的最大值
		// 如果ObjectBlock存在对此文件的引用，则至少为1
		needRepCount := mymath.Max(repMaxCnt, mymath.Min(1, blkCnt))

		repNodes, err := execCtx.Args.DB.Cache().GetCachingFileNodes(tx, fileHash)
		if err != nil {
			return fmt.Errorf("get caching file nodes failed, err: %w", err)
		}

		allNodes, err := mysql.Node.GetAllNodes(tx)
		if err != nil {
			return fmt.Errorf("get all nodes failed, err: %w", err)
		}

		var normalNodes, unavaiNodes []model.Node
		for _, node := range repNodes {
			if node.State == consts.NODE_STATE_NORMAL {
				normalNodes = append(normalNodes, node)
			} else if node.State == consts.NODE_STATE_UNAVAILABLE {
				unavaiNodes = append(unavaiNodes, node)
			}
		}

		// 如果Available的备份数超过期望备份数，则让一些节点退出
		if len(normalNodes) > needRepCount {
			delNodes := chooseDeleteAvaiRepNodes(allNodes, normalNodes, len(normalNodes)-needRepCount)
			for _, node := range delNodes {
				err := execCtx.Args.DB.Cache().SetTemp(tx, fileHash, node.NodeID)
				if err != nil {
					return fmt.Errorf("change cache state failed, err: %w", err)
				}
				updatedNodeIDs = append(updatedNodeIDs, node.NodeID)
			}
			return nil
		}

		minAvaiNodeCnt := int(math.Ceil(float64(config.Cfg().MinAvailableRepProportion) * float64(needRepCount)))

		// 因为总备份数不够，而需要增加的备份数
		add1 := mymath.Max(0, needRepCount-len(repNodes))

		// 因为Available的备份数占比过少，而需要增加的备份数
		add2 := mymath.Max(0, minAvaiNodeCnt-len(normalNodes))

		// 最终需要增加的备份数，是以上两种情况的最大值
		finalAddCount := mymath.Max(add1, add2)

		if finalAddCount > 0 {
			newNodes := chooseNewRepNodes(allNodes, repNodes, finalAddCount)
			if len(newNodes) < finalAddCount {
				log.WithField("FileHash", fileHash).Warnf("need %d more rep nodes, but get only %d nodes", finalAddCount, len(newNodes))
				// TODO 节点数不够，进行一个告警
			}

			for _, node := range newNodes {
				err := execCtx.Args.DB.Cache().CreatePinned(tx, fileHash, node.NodeID)
				if err != nil {
					return fmt.Errorf("create cache failed, err: %w", err)
				}
				updatedNodeIDs = append(updatedNodeIDs, node.NodeID)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return updatedNodeIDs, nil
}

func chooseNewRepNodes(allNodes []model.Node, curRepNodes []model.Node, newCount int) []model.Node {
	noRepNodes := lo.Reject(allNodes, func(node model.Node, index int) bool {
		return lo.ContainsBy(curRepNodes, func(n model.Node) bool { return node.NodeID == n.NodeID }) ||
			node.State != consts.NODE_STATE_NORMAL
	})

	repNodeLocationIDs := make(map[int]bool)
	for _, node := range curRepNodes {
		repNodeLocationIDs[node.LocationID] = true
	}

	mysort.Sort(noRepNodes, func(l, r model.Node) int {
		// LocationID不存在时为false，false - true < 0，所以LocationID不存在的会排在前面
		return mysort.CmpBool(repNodeLocationIDs[l.LocationID], repNodeLocationIDs[r.LocationID])
	})

	return noRepNodes[:mymath.Min(newCount, len(noRepNodes))]
}

func chooseDeleteAvaiRepNodes(allNodes []model.Node, curAvaiRepNodes []model.Node, delCount int) []model.Node {
	// 按照地域ID分组
	locationGroupedNodes := make(map[int][]model.Node)
	for _, node := range curAvaiRepNodes {
		nodes := locationGroupedNodes[node.LocationID]
		nodes = append(nodes, node)
		locationGroupedNodes[node.LocationID] = nodes
	}

	// 每次从每个分组中取出一个元素放入结果数组，并将这个元素从分组中删除
	// 最后结果数组中的元素会按照地域交错循环排列，比如：ABCABCBCC。同时还有一个特征：靠后的循环节中的元素都来自于元素数多的分组
	// 将结果数组反转（此处是用存放时就逆序的形式实现），就把元素数多的分组提前了，此时从头部取出要删除的节点即可
	alternatedNodes := make([]model.Node, len(curAvaiRepNodes))
	for i := len(curAvaiRepNodes) - 1; i >= 0; {
		for id, nodes := range locationGroupedNodes {
			alternatedNodes[i] = nodes[0]

			if len(nodes) == 1 {
				delete(locationGroupedNodes, id)
			} else {
				locationGroupedNodes[id] = nodes[1:]
			}

			// 放置一个元素就移动一下下一个存放点
			i--
		}
	}

	return alternatedNodes[:mymath.Min(delCount, len(alternatedNodes))]
}

func init() {
	RegisterMessageConvertor(func(msg scevt.CheckRepCount) Event { return NewCheckRepCount(msg.FileHashes) })
}
