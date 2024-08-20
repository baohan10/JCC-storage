package parser

import (
	"fmt"
	"math"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/plan"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/plan/ops"
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/lo2"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch2/ops2"
)

type DefaultParser struct {
	EC cdssdk.ECRedundancy
}

func NewParser(ec cdssdk.ECRedundancy) *DefaultParser {
	return &DefaultParser{
		EC: ec,
	}
}

type ParseContext struct {
	Ft  ioswitch2.FromTo
	DAG *dag.Graph
	// 为了产生所有To所需的数据范围，而需要From打开的范围。
	// 这个范围是基于整个文件的，且上下界都取整到条带大小的整数倍，因此上界是有可能超过文件大小的。
	StreamRange exec.Range
}

func (p *DefaultParser) Parse(ft ioswitch2.FromTo, blder *exec.PlanBuilder) error {
	ctx := ParseContext{Ft: ft}

	// 分成两个阶段：
	// 1. 基于From和To生成更多指令，初步匹配to的需求

	// 计算一下打开流的范围
	p.calcStreamRange(&ctx)

	err := p.extend(&ctx, ft)
	if err != nil {
		return err
	}

	// 2. 优化上一步生成的指令

	// 对于删除指令的优化，需要反复进行，直到没有变化为止。
	// 从目前实现上来说不会死循环
	for {
		opted := false
		if p.removeUnusedJoin(&ctx) {
			opted = true
		}
		if p.removeUnusedMultiplyOutput(&ctx) {
			opted = true
		}
		if p.removeUnusedSplit(&ctx) {
			opted = true
		}
		if p.omitSplitJoin(&ctx) {
			opted = true
		}

		if !opted {
			break
		}
	}

	// 确定指令执行位置的过程，也需要反复进行，直到没有变化为止。
	for p.pin(&ctx) {
	}

	// 下面这些只需要执行一次，但需要按顺序
	p.dropUnused(&ctx)
	p.storeIPFSWriteResult(&ctx)
	p.generateClone(&ctx)
	p.generateRange(&ctx)

	return plan.Generate(ctx.DAG, blder)
}
func (p *DefaultParser) findOutputStream(ctx *ParseContext, streamIndex int) *dag.StreamVar {
	var ret *dag.StreamVar
	ctx.DAG.Walk(func(n *dag.Node) bool {
		for _, o := range n.OutputStreams {
			if o != nil && ioswitch2.SProps(o).StreamIndex == streamIndex {
				ret = o
				return false
			}
		}
		return true
	})

	return ret
}

// 计算输入流的打开范围。会把流的范围按条带大小取整
func (p *DefaultParser) calcStreamRange(ctx *ParseContext) {
	stripSize := int64(p.EC.ChunkSize * p.EC.K)

	rng := exec.Range{
		Offset: math.MaxInt64,
	}

	for _, to := range ctx.Ft.Toes {
		if to.GetDataIndex() == -1 {
			toRng := to.GetRange()
			rng.ExtendStart(math2.Floor(toRng.Offset, stripSize))
			if toRng.Length != nil {
				rng.ExtendEnd(math2.Ceil(toRng.Offset+*toRng.Length, stripSize))
			} else {
				rng.Length = nil
			}

		} else {
			toRng := to.GetRange()

			blkStartIndex := math2.FloorDiv(toRng.Offset, int64(p.EC.ChunkSize))
			rng.ExtendStart(blkStartIndex * stripSize)
			if toRng.Length != nil {
				blkEndIndex := math2.CeilDiv(toRng.Offset+*toRng.Length, int64(p.EC.ChunkSize))
				rng.ExtendEnd(blkEndIndex * stripSize)
			} else {
				rng.Length = nil
			}
		}
	}

	ctx.StreamRange = rng
}

func (p *DefaultParser) extend(ctx *ParseContext, ft ioswitch2.FromTo) error {
	for _, fr := range ft.Froms {
		_, err := p.buildFromNode(ctx, &ft, fr)
		if err != nil {
			return err
		}

		// 对于完整文件的From，生成Split指令
		if fr.GetDataIndex() == -1 {
			n, _ := dag.NewNode(ctx.DAG, &ops2.ChunkedSplitType{ChunkSize: p.EC.ChunkSize, OutputCount: p.EC.K}, &ioswitch2.NodeProps{})
			for i := 0; i < p.EC.K; i++ {
				ioswitch2.SProps(n.OutputStreams[i]).StreamIndex = i
			}
		}
	}

	// 如果有K个不同的文件块流，则生成Multiply指令，同时针对其生成的流，生成Join指令
	ecInputStrs := make(map[int]*dag.StreamVar)
loop:
	for _, o := range ctx.DAG.Nodes {
		for _, s := range o.OutputStreams {
			prop := ioswitch2.SProps(s)
			if prop.StreamIndex >= 0 && ecInputStrs[prop.StreamIndex] == nil {
				ecInputStrs[prop.StreamIndex] = s
				if len(ecInputStrs) == p.EC.K {
					break loop
				}
			}
		}
	}
	if len(ecInputStrs) == p.EC.K {
		mulNode, mulType := dag.NewNode(ctx.DAG, &ops2.MultiplyType{
			EC: p.EC,
		}, &ioswitch2.NodeProps{})

		for _, s := range ecInputStrs {
			mulType.AddInput(mulNode, s)
		}
		for i := 0; i < p.EC.N; i++ {
			mulType.NewOutput(mulNode, i)
		}

		joinNode, _ := dag.NewNode(ctx.DAG, &ops2.ChunkedJoinType{
			InputCount: p.EC.K,
			ChunkSize:  p.EC.ChunkSize,
		}, &ioswitch2.NodeProps{})

		for i := 0; i < p.EC.K; i++ {
			// 不可能找不到流
			p.findOutputStream(ctx, i).To(joinNode, i)
		}
		ioswitch2.SProps(joinNode.OutputStreams[0]).StreamIndex = -1
	}

	// 为每一个To找到一个输入流
	for _, to := range ft.Toes {
		n, err := p.buildToNode(ctx, &ft, to)
		if err != nil {
			return err
		}

		str := p.findOutputStream(ctx, to.GetDataIndex())
		if str == nil {
			return fmt.Errorf("no output stream found for data index %d", to.GetDataIndex())
		}

		str.To(n, 0)
	}

	return nil
}

func (p *DefaultParser) buildFromNode(ctx *ParseContext, ft *ioswitch2.FromTo, f ioswitch2.From) (*dag.Node, error) {
	var repRange exec.Range
	var blkRange exec.Range

	repRange.Offset = ctx.StreamRange.Offset
	blkRange.Offset = ctx.StreamRange.Offset / int64(p.EC.ChunkSize*p.EC.K) * int64(p.EC.ChunkSize)
	if ctx.StreamRange.Length != nil {
		repRngLen := *ctx.StreamRange.Length
		repRange.Length = &repRngLen

		blkRngLen := *ctx.StreamRange.Length / int64(p.EC.ChunkSize*p.EC.K) * int64(p.EC.ChunkSize)
		blkRange.Length = &blkRngLen
	}

	switch f := f.(type) {
	case *ioswitch2.FromNode:
		n, t := dag.NewNode(ctx.DAG, &ops2.IPFSReadType{
			FileHash: f.FileHash,
			Option: ipfs.ReadOption{
				Offset: 0,
				Length: -1,
			},
		}, &ioswitch2.NodeProps{
			From: f,
		})
		ioswitch2.SProps(n.OutputStreams[0]).StreamIndex = f.DataIndex

		if f.DataIndex == -1 {
			t.Option.Offset = repRange.Offset
			if repRange.Length != nil {
				t.Option.Length = *repRange.Length
			}
		} else {
			t.Option.Offset = blkRange.Offset
			if blkRange.Length != nil {
				t.Option.Length = *blkRange.Length
			}
		}

		if f.Node != nil {
			n.Env.ToEnvWorker(&ioswitch2.AgentWorker{Node: *f.Node})
		}

		return n, nil

	case *ioswitch2.FromDriver:
		n, _ := dag.NewNode(ctx.DAG, &ops.FromDriverType{Handle: f.Handle}, &ioswitch2.NodeProps{From: f})
		n.Env.ToEnvDriver()
		ioswitch2.SProps(n.OutputStreams[0]).StreamIndex = f.DataIndex

		if f.DataIndex == -1 {
			f.Handle.RangeHint.Offset = repRange.Offset
			f.Handle.RangeHint.Length = repRange.Length
		} else {
			f.Handle.RangeHint.Offset = blkRange.Offset
			f.Handle.RangeHint.Length = blkRange.Length
		}

		return n, nil

	default:
		return nil, fmt.Errorf("unsupported from type %T", f)
	}
}

func (p *DefaultParser) buildToNode(ctx *ParseContext, ft *ioswitch2.FromTo, t ioswitch2.To) (*dag.Node, error) {
	switch t := t.(type) {
	case *ioswitch2.ToNode:
		n, _ := dag.NewNode(ctx.DAG, &ops2.IPFSWriteType{
			FileHashStoreKey: t.FileHashStoreKey,
			Range:            t.Range,
		}, &ioswitch2.NodeProps{
			To: t,
		})

		return n, nil

	case *ioswitch2.ToDriver:
		n, _ := dag.NewNode(ctx.DAG, &ops.ToDriverType{Handle: t.Handle, Range: t.Range}, &ioswitch2.NodeProps{To: t})
		n.Env.ToEnvDriver()

		return n, nil

	default:
		return nil, fmt.Errorf("unsupported to type %T", t)
	}
}

// 删除输出流未被使用的Join指令
func (p *DefaultParser) removeUnusedJoin(ctx *ParseContext) bool {
	changed := false

	dag.WalkOnlyType[*ops2.ChunkedJoinType](ctx.DAG, func(node *dag.Node, typ *ops2.ChunkedJoinType) bool {
		if len(node.OutputStreams[0].Toes) > 0 {
			return true
		}

		for _, in := range node.InputStreams {
			in.NotTo(node)
		}

		ctx.DAG.RemoveNode(node)
		return true
	})

	return changed
}

// 减少未使用的Multiply指令的输出流。如果减少到0，则删除该指令
func (p *DefaultParser) removeUnusedMultiplyOutput(ctx *ParseContext) bool {
	changed := false
	dag.WalkOnlyType[*ops2.MultiplyType](ctx.DAG, func(node *dag.Node, typ *ops2.MultiplyType) bool {
		for i2, out := range node.OutputStreams {
			if len(out.Toes) > 0 {
				continue
			}

			node.OutputStreams[i2] = nil
			changed = true
		}
		node.OutputStreams = lo2.RemoveAllDefault(node.OutputStreams)

		// 如果所有输出流都被删除，则删除该指令
		if len(node.OutputStreams) == 0 {
			for _, in := range node.InputStreams {
				in.NotTo(node)
			}

			ctx.DAG.RemoveNode(node)
			changed = true
		}

		return true
	})
	return changed
}

// 删除未使用的Split指令
func (p *DefaultParser) removeUnusedSplit(ctx *ParseContext) bool {
	changed := false
	dag.WalkOnlyType[*ops2.ChunkedSplitType](ctx.DAG, func(node *dag.Node, typ *ops2.ChunkedSplitType) bool {
		// Split出来的每一个流都没有被使用，才能删除这个指令
		for _, out := range node.OutputStreams {
			if len(out.Toes) > 0 {
				return true
			}
		}

		node.InputStreams[0].NotTo(node)
		ctx.DAG.RemoveNode(node)
		changed = true
		return true
	})

	return changed
}

// 如果Split的结果被完全用于Join，则省略Split和Join指令
func (p *DefaultParser) omitSplitJoin(ctx *ParseContext) bool {
	changed := false

	dag.WalkOnlyType[*ops2.ChunkedSplitType](ctx.DAG, func(splitNode *dag.Node, typ *ops2.ChunkedSplitType) bool {
		// Split指令的每一个输出都有且只有一个目的地
		var joinNode *dag.Node
		for _, out := range splitNode.OutputStreams {
			if len(out.Toes) != 1 {
				continue
			}

			if joinNode == nil {
				joinNode = out.Toes[0].Node
			} else if joinNode != out.Toes[0].Node {
				return true
			}
		}

		if joinNode == nil {
			return true
		}

		// 且这个目的地要是一个Join指令
		_, ok := joinNode.Type.(*ops2.ChunkedJoinType)
		if !ok {
			return true
		}

		// 同时这个Join指令的输入也必须全部来自Split指令的输出。
		// 由于上面判断了Split指令的输出目的地都相同，所以这里只要判断Join指令的输入数量是否与Split指令的输出数量相同即可
		if len(joinNode.InputStreams) != len(splitNode.OutputStreams) {
			return true
		}

		// 所有条件都满足，可以开始省略操作，将Join操作的目的地的输入流替换为Split操作的输入流：
		// F->Split->Join->T 变换为：F->T
		splitNode.InputStreams[0].NotTo(splitNode)
		for _, out := range joinNode.OutputStreams[0].Toes {
			splitNode.InputStreams[0].To(out.Node, out.SlotIndex)
		}

		// 并删除这两个指令
		ctx.DAG.RemoveNode(joinNode)
		ctx.DAG.RemoveNode(splitNode)

		changed = true
		return true
	})

	return changed
}

// 通过流的输入输出位置来确定指令的执行位置。
// To系列的指令都会有固定的执行位置，这些位置会随着pin操作逐步扩散到整个DAG，
// 所以理论上不会出现有指令的位置始终无法确定的情况。
func (p *DefaultParser) pin(ctx *ParseContext) bool {
	changed := false
	ctx.DAG.Walk(func(node *dag.Node) bool {
		var toEnv *dag.NodeEnv
		for _, out := range node.OutputStreams {
			for _, to := range out.Toes {
				if to.Node.Env.Type == dag.EnvUnknown {
					continue
				}

				if toEnv == nil {
					toEnv = &to.Node.Env
				} else if !toEnv.Equals(to.Node.Env) {
					toEnv = nil
					break
				}
			}
		}

		if toEnv != nil {
			if !node.Env.Equals(*toEnv) {
				changed = true
			}

			node.Env = *toEnv
			return true
		}

		// 否则根据输入流的始发地来固定
		var fromEnv *dag.NodeEnv
		for _, in := range node.InputStreams {
			if in.From.Node.Env.Type == dag.EnvUnknown {
				continue
			}

			if fromEnv == nil {
				fromEnv = &in.From.Node.Env
			} else if !fromEnv.Equals(in.From.Node.Env) {
				fromEnv = nil
				break
			}
		}

		if fromEnv != nil {
			if !node.Env.Equals(*fromEnv) {
				changed = true
			}

			node.Env = *fromEnv
		}
		return true
	})

	return changed
}

// 对于所有未使用的流，增加Drop指令
func (p *DefaultParser) dropUnused(ctx *ParseContext) {
	ctx.DAG.Walk(func(node *dag.Node) bool {
		for _, out := range node.OutputStreams {
			if len(out.Toes) == 0 {
				n := ctx.DAG.NewNode(&ops.DropType{}, &ioswitch2.NodeProps{})
				n.Env = node.Env
				out.To(n, 0)
			}
		}
		return true
	})
}

// 为IPFS写入指令存储结果
func (p *DefaultParser) storeIPFSWriteResult(ctx *ParseContext) {
	dag.WalkOnlyType[*ops2.IPFSWriteType](ctx.DAG, func(node *dag.Node, typ *ops2.IPFSWriteType) bool {
		if typ.FileHashStoreKey == "" {
			return true
		}

		n := ctx.DAG.NewNode(&ops.StoreType{
			StoreKey: typ.FileHashStoreKey,
		}, &ioswitch2.NodeProps{})
		n.Env.ToEnvDriver()

		node.OutputValues[0].To(n, 0)
		return true
	})
}

// 生成Range指令。StreamRange可能超过文件总大小，但Range指令会在数据量不够时不报错而是正常返回
func (p *DefaultParser) generateRange(ctx *ParseContext) {
	ctx.DAG.Walk(func(node *dag.Node) bool {
		props := ioswitch2.NProps(node)
		if props.To == nil {
			return true
		}

		toDataIdx := props.To.GetDataIndex()
		toRng := props.To.GetRange()

		if toDataIdx == -1 {
			n := ctx.DAG.NewNode(&ops2.RangeType{
				Range: exec.Range{
					Offset: toRng.Offset - ctx.StreamRange.Offset,
					Length: toRng.Length,
				},
			}, &ioswitch2.NodeProps{})
			n.Env = node.InputStreams[0].From.Node.Env

			node.InputStreams[0].To(n, 0)
			node.InputStreams[0].NotTo(node)
			n.OutputStreams[0].To(node, 0)

		} else {
			stripSize := int64(p.EC.ChunkSize * p.EC.K)
			blkStartIdx := ctx.StreamRange.Offset / stripSize

			blkStart := blkStartIdx * int64(p.EC.ChunkSize)

			n := ctx.DAG.NewNode(&ops2.RangeType{
				Range: exec.Range{
					Offset: toRng.Offset - blkStart,
					Length: toRng.Length,
				},
			}, &ioswitch2.NodeProps{})
			n.Env = node.InputStreams[0].From.Node.Env

			node.InputStreams[0].To(n, 0)
			node.InputStreams[0].NotTo(node)
			n.OutputStreams[0].To(node, 0)
		}

		return true
	})
}

// 生成Clone指令
func (p *DefaultParser) generateClone(ctx *ParseContext) {
	ctx.DAG.Walk(func(node *dag.Node) bool {
		for _, out := range node.OutputStreams {
			if len(out.Toes) <= 1 {
				continue
			}

			n, t := dag.NewNode(ctx.DAG, &ops2.CloneStreamType{}, &ioswitch2.NodeProps{})
			n.Env = node.Env
			for _, to := range out.Toes {
				t.NewOutput(node).To(to.Node, to.SlotIndex)
			}
			out.Toes = nil
			out.To(n, 0)
		}

		for _, out := range node.OutputValues {
			if len(out.Toes) <= 1 {
				continue
			}

			n, t := dag.NewNode(ctx.DAG, &ops2.CloneVarType{}, &ioswitch2.NodeProps{})
			n.Env = node.Env
			for _, to := range out.Toes {
				t.NewOutput(node).To(to.Node, to.SlotIndex)
			}
			out.Toes = nil
			out.To(n, 0)
		}

		return true
	})
}
