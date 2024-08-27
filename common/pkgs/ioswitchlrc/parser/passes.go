package parser

import (
	"fmt"
	"math"

	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/plan/ops"
	"gitlink.org.cn/cloudream/common/pkgs/ipfs"
	"gitlink.org.cn/cloudream/common/utils/math2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitchlrc"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitchlrc/ops2"
)

// 计算输入流的打开范围。会把流的范围按条带大小取整
func calcStreamRange(ctx *GenerateContext) {
	stripSize := int64(ctx.LRC.ChunkSize * ctx.LRC.K)

	rng := exec.Range{
		Offset: math.MaxInt64,
	}

	for _, to := range ctx.Toes {
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

			blkStartIndex := math2.FloorDiv(toRng.Offset, int64(ctx.LRC.ChunkSize))
			rng.ExtendStart(blkStartIndex * stripSize)
			if toRng.Length != nil {
				blkEndIndex := math2.CeilDiv(toRng.Offset+*toRng.Length, int64(ctx.LRC.ChunkSize))
				rng.ExtendEnd(blkEndIndex * stripSize)
			} else {
				rng.Length = nil
			}
		}
	}

	ctx.StreamRange = rng
}

func buildFromNode(ctx *GenerateContext, f ioswitchlrc.From) (*dag.Node, error) {
	var repRange exec.Range
	var blkRange exec.Range

	repRange.Offset = ctx.StreamRange.Offset
	blkRange.Offset = ctx.StreamRange.Offset / int64(ctx.LRC.ChunkSize*ctx.LRC.K) * int64(ctx.LRC.ChunkSize)
	if ctx.StreamRange.Length != nil {
		repRngLen := *ctx.StreamRange.Length
		repRange.Length = &repRngLen

		blkRngLen := *ctx.StreamRange.Length / int64(ctx.LRC.ChunkSize*ctx.LRC.K) * int64(ctx.LRC.ChunkSize)
		blkRange.Length = &blkRngLen
	}

	switch f := f.(type) {
	case *ioswitchlrc.FromNode:
		n, t := dag.NewNode(ctx.DAG, &ops2.IPFSReadType{
			FileHash: f.FileHash,
			Option: ipfs.ReadOption{
				Offset: 0,
				Length: -1,
			},
		}, &ioswitchlrc.NodeProps{
			From: f,
		})
		ioswitchlrc.SProps(n.OutputStreams[0]).StreamIndex = f.DataIndex

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
			n.Env.ToEnvWorker(&ioswitchlrc.AgentWorker{Node: *f.Node})
			n.Env.Pinned = true
		}

		return n, nil

	case *ioswitchlrc.FromDriver:
		n, _ := dag.NewNode(ctx.DAG, &ops.FromDriverType{Handle: f.Handle}, &ioswitchlrc.NodeProps{From: f})
		n.Env.ToEnvDriver()
		n.Env.Pinned = true
		ioswitchlrc.SProps(n.OutputStreams[0]).StreamIndex = f.DataIndex

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

func buildToNode(ctx *GenerateContext, t ioswitchlrc.To) (*dag.Node, error) {
	switch t := t.(type) {
	case *ioswitchlrc.ToNode:
		n, _ := dag.NewNode(ctx.DAG, &ops2.IPFSWriteType{
			FileHashStoreKey: t.FileHashStoreKey,
			Range:            t.Range,
		}, &ioswitchlrc.NodeProps{
			To: t,
		})
		n.Env.ToEnvWorker(&ioswitchlrc.AgentWorker{Node: t.Node})
		n.Env.Pinned = true

		return n, nil

	case *ioswitchlrc.ToDriver:
		n, _ := dag.NewNode(ctx.DAG, &ops.ToDriverType{Handle: t.Handle, Range: t.Range}, &ioswitchlrc.NodeProps{To: t})
		n.Env.ToEnvDriver()
		n.Env.Pinned = true

		return n, nil

	default:
		return nil, fmt.Errorf("unsupported to type %T", t)
	}
}

// 通过流的输入输出位置来确定指令的执行位置。
// To系列的指令都会有固定的执行位置，这些位置会随着pin操作逐步扩散到整个DAG，
// 所以理论上不会出现有指令的位置始终无法确定的情况。
func pin(ctx *GenerateContext) bool {
	changed := false
	ctx.DAG.Walk(func(node *dag.Node) bool {
		if node.Env.Pinned {
			return true
		}

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
func dropUnused(ctx *GenerateContext) {
	ctx.DAG.Walk(func(node *dag.Node) bool {
		for _, out := range node.OutputStreams {
			if len(out.Toes) == 0 {
				n := ctx.DAG.NewNode(&ops.DropType{}, &ioswitchlrc.NodeProps{})
				n.Env = node.Env
				out.To(n, 0)
			}
		}
		return true
	})
}

// 为IPFS写入指令存储结果
func storeIPFSWriteResult(ctx *GenerateContext) {
	dag.WalkOnlyType[*ops2.IPFSWriteType](ctx.DAG, func(node *dag.Node, typ *ops2.IPFSWriteType) bool {
		if typ.FileHashStoreKey == "" {
			return true
		}

		n := ctx.DAG.NewNode(&ops.StoreType{
			StoreKey: typ.FileHashStoreKey,
		}, &ioswitchlrc.NodeProps{})
		n.Env.ToEnvDriver()

		node.OutputValues[0].To(n, 0)
		return true
	})
}

// 生成Range指令。StreamRange可能超过文件总大小，但Range指令会在数据量不够时不报错而是正常返回
func generateRange(ctx *GenerateContext) {
	ctx.DAG.Walk(func(node *dag.Node) bool {
		props := ioswitchlrc.NProps(node)
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
			}, &ioswitchlrc.NodeProps{})
			n.Env = node.InputStreams[0].From.Node.Env

			node.InputStreams[0].To(n, 0)
			node.InputStreams[0].NotTo(node)
			n.OutputStreams[0].To(node, 0)

		} else {
			stripSize := int64(ctx.LRC.ChunkSize * ctx.LRC.K)
			blkStartIdx := ctx.StreamRange.Offset / stripSize

			blkStart := blkStartIdx * int64(ctx.LRC.ChunkSize)

			n := ctx.DAG.NewNode(&ops2.RangeType{
				Range: exec.Range{
					Offset: toRng.Offset - blkStart,
					Length: toRng.Length,
				},
			}, &ioswitchlrc.NodeProps{})
			n.Env = node.InputStreams[0].From.Node.Env

			node.InputStreams[0].To(n, 0)
			node.InputStreams[0].NotTo(node)
			n.OutputStreams[0].To(node, 0)
		}

		return true
	})
}

// 生成Clone指令
func generateClone(ctx *GenerateContext) {
	ctx.DAG.Walk(func(node *dag.Node) bool {
		for _, out := range node.OutputStreams {
			if len(out.Toes) <= 1 {
				continue
			}

			n, t := dag.NewNode(ctx.DAG, &ops2.CloneStreamType{}, &ioswitchlrc.NodeProps{})
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

			n, t := dag.NewNode(ctx.DAG, &ops2.CloneVarType{}, &ioswitchlrc.NodeProps{})
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
