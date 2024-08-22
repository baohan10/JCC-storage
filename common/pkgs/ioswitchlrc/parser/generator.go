package parser

import (
	"fmt"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/plan"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitchlrc"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitchlrc/ops2"
)

type GenerateContext struct {
	LRC         cdssdk.LRCRedundancy
	DAG         *dag.Graph
	Toes        []ioswitchlrc.To
	StreamRange exec.Range
}

// 输入一个完整文件，从这个完整文件产生任意文件块（也可再产生完整文件）。
func Encode(fr ioswitchlrc.From, toes []ioswitchlrc.To, blder *exec.PlanBuilder) error {
	if fr.GetDataIndex() != -1 {
		return fmt.Errorf("from data is not a complete file")
	}

	ctx := GenerateContext{
		LRC:  cdssdk.DefaultLRCRedundancy,
		DAG:  dag.NewGraph(),
		Toes: toes,
	}

	calcStreamRange(&ctx)
	err := buildDAGEncode(&ctx, fr, toes)
	if err != nil {
		return err
	}

	// 确定指令执行位置的过程，也需要反复进行，直到没有变化为止。
	for pin(&ctx) {
	}

	// 下面这些只需要执行一次，但需要按顺序
	dropUnused(&ctx)
	storeIPFSWriteResult(&ctx)
	generateClone(&ctx)
	generateRange(&ctx)

	return plan.Generate(ctx.DAG, blder)
}

func buildDAGEncode(ctx *GenerateContext, fr ioswitchlrc.From, toes []ioswitchlrc.To) error {
	frNode, err := buildFromNode(ctx, fr)
	if err != nil {
		return fmt.Errorf("building from node: %w", err)
	}

	var dataToes []ioswitchlrc.To
	var parityToes []ioswitchlrc.To

	// 先创建需要完整文件的To节点，同时统计一下需要哪些文件块
	for _, to := range toes {
		if to.GetDataIndex() != -1 {
			continue
		}

		toNode, err := buildToNode(ctx, to)
		if err != nil {
			return fmt.Errorf("building to node: %w", err)
		}

		idx := to.GetDataIndex()
		if idx == -1 {
			frNode.OutputStreams[0].To(toNode, 0)
		} else if idx < ctx.LRC.K {
			dataToes = append(dataToes, to)
		} else {
			parityToes = append(parityToes, to)
		}
	}

	if len(dataToes) == 0 && len(parityToes) == 0 {
		return nil
	}

	// 需要文件块，则生成Split指令
	splitNode := ctx.DAG.NewNode(&ops2.ChunkedSplitType{
		OutputCount: ctx.LRC.K,
		ChunkSize:   ctx.LRC.ChunkSize,
	}, nil)

	for _, to := range dataToes {
		toNode, err := buildToNode(ctx, to)
		if err != nil {
			return fmt.Errorf("building to node: %w", err)
		}

		splitNode.OutputStreams[to.GetDataIndex()].To(toNode, 0)
	}

	if len(parityToes) == 0 {
		return nil
	}

	// 需要校验块，则进一步生成Construct指令

	conNode, conType := dag.NewNode(ctx.DAG, &ops2.LRCConstructAnyType{
		LRC: ctx.LRC,
	}, nil)

	for _, out := range splitNode.OutputStreams {
		conType.AddInput(conNode, out)
	}

	for _, to := range parityToes {
		toNode, err := buildToNode(ctx, to)
		if err != nil {
			return fmt.Errorf("building to node: %w", err)
		}

		conType.NewOutput(conNode, to.GetDataIndex()).To(toNode, 0)
	}
	return nil
}

// 提供数据块+编码块中的k个块，重建任意块，包括完整文件。
func ReconstructAny(frs []ioswitchlrc.From, toes []ioswitchlrc.To, blder *exec.PlanBuilder) error {
	ctx := GenerateContext{
		LRC:  cdssdk.DefaultLRCRedundancy,
		DAG:  dag.NewGraph(),
		Toes: toes,
	}

	calcStreamRange(&ctx)
	err := buildDAGReconstructAny(&ctx, frs, toes)
	if err != nil {
		return err
	}

	// 确定指令执行位置的过程，也需要反复进行，直到没有变化为止。
	for pin(&ctx) {
	}

	// 下面这些只需要执行一次，但需要按顺序
	dropUnused(&ctx)
	storeIPFSWriteResult(&ctx)
	generateClone(&ctx)
	generateRange(&ctx)

	return plan.Generate(ctx.DAG, blder)
}

func buildDAGReconstructAny(ctx *GenerateContext, frs []ioswitchlrc.From, toes []ioswitchlrc.To) error {
	frNodes := make(map[int]*dag.Node)
	for _, fr := range frs {
		frNode, err := buildFromNode(ctx, fr)
		if err != nil {
			return fmt.Errorf("building from node: %w", err)
		}

		frNodes[fr.GetDataIndex()] = frNode
	}

	var completeToes []ioswitchlrc.To
	var missedToes []ioswitchlrc.To

	// 先创建需要完整文件的To节点，同时统计一下需要哪些文件块
	for _, to := range toes {
		toIdx := to.GetDataIndex()
		fr := frNodes[toIdx]
		if fr != nil {
			node, err := buildToNode(ctx, to)
			if err != nil {
				return fmt.Errorf("building to node: %w", err)
			}

			fr.OutputStreams[0].To(node, 0)
			continue
		}

		if toIdx == -1 {
			completeToes = append(completeToes, to)
		} else {
			missedToes = append(missedToes, to)
		}
	}

	if len(completeToes) == 0 && len(missedToes) == 0 {
		return nil
	}

	// 生成Construct指令来恢复缺少的块

	conNode, conType := dag.NewNode(ctx.DAG, &ops2.LRCConstructAnyType{
		LRC: ctx.LRC,
	}, nil)

	for _, fr := range frNodes {
		conType.AddInput(conNode, fr.OutputStreams[0])
	}

	for _, to := range missedToes {
		toNode, err := buildToNode(ctx, to)
		if err != nil {
			return fmt.Errorf("building to node: %w", err)
		}

		conType.NewOutput(conNode, to.GetDataIndex()).To(toNode, 0)
	}

	if len(completeToes) == 0 {
		return nil
	}

	// 需要完整文件，则生成Join指令

	joinNode := ctx.DAG.NewNode(&ops2.ChunkedJoinType{
		InputCount: ctx.LRC.K,
		ChunkSize:  ctx.LRC.ChunkSize,
	}, nil)

	for i := 0; i < ctx.LRC.K; i++ {
		n := frNodes[i]
		if n == nil {
			conType.NewOutput(conNode, i).To(joinNode, i)
		} else {
			n.OutputStreams[0].To(joinNode, i)
		}
	}

	for _, to := range completeToes {
		toNode, err := buildToNode(ctx, to)
		if err != nil {
			return fmt.Errorf("building to node: %w", err)
		}

		joinNode.OutputStreams[0].To(toNode, 0)
	}

	return nil
}

// 输入同一组的多个块，恢复出剩下缺少的一个块。
func ReconstructGroup(frs []ioswitchlrc.From, toes []ioswitchlrc.To, blder *exec.PlanBuilder) error {
	ctx := GenerateContext{
		LRC:  cdssdk.DefaultLRCRedundancy,
		DAG:  dag.NewGraph(),
		Toes: toes,
	}

	calcStreamRange(&ctx)
	err := buildDAGReconstructGroup(&ctx, frs, toes)
	if err != nil {
		return err
	}

	// 确定指令执行位置的过程，也需要反复进行，直到没有变化为止。
	for pin(&ctx) {
	}

	// 下面这些只需要执行一次，但需要按顺序
	dropUnused(&ctx)
	storeIPFSWriteResult(&ctx)
	generateClone(&ctx)
	generateRange(&ctx)

	return plan.Generate(ctx.DAG, blder)
}

func buildDAGReconstructGroup(ctx *GenerateContext, frs []ioswitchlrc.From, toes []ioswitchlrc.To) error {
	missedGrpIdx := toes[0].GetDataIndex()

	conNode := ctx.DAG.NewNode(&ops2.LRCConstructGroupType{
		LRC:              ctx.LRC,
		TargetBlockIndex: missedGrpIdx,
	}, nil)

	for i, fr := range frs {
		frNode, err := buildFromNode(ctx, fr)
		if err != nil {
			return fmt.Errorf("building from node: %w", err)
		}

		frNode.OutputStreams[0].To(conNode, i)
	}

	for _, to := range toes {
		toNode, err := buildToNode(ctx, to)
		if err != nil {
			return fmt.Errorf("building to node: %w", err)
		}

		conNode.OutputStreams[0].To(toNode, 0)
	}

	return nil
}
