package plans

import (
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/ops"
)

func (b *AgentPlanBuilder) GRCPFetch(node model.Node, str *AgentStream) *AgentStream {
	agtStr := &AgentStream{
		owner: b,
		info:  b.owner.newStream(),
	}

	b.ops = append(b.ops, &ops.GRPCFetch{
		RemoteID: str.info.ID,
		LocalID:  agtStr.info.ID,
		Node:     node,
	})

	return agtStr
}

func (s *AgentStream) GRPCSend(node model.Node) *AgentStream {
	agtStr := &AgentStream{
		owner: s.owner.owner.AtAgent(node),
		info:  s.owner.owner.newStream(),
	}

	s.owner.ops = append(s.owner.ops, &ops.GRPCSend{
		LocalID:  s.info.ID,
		RemoteID: agtStr.info.ID,
		Node:     node,
	})

	return agtStr
}

func (b *AgentPlanBuilder) FileRead(filePath string) *AgentStream {
	agtStr := &AgentStream{
		owner: b,
		info:  b.owner.newStream(),
	}

	b.ops = append(b.ops, &ops.FileRead{
		OutputID: agtStr.info.ID,
		FilePath: filePath,
	})

	return agtStr
}

func (b *AgentStream) FileWrite(filePath string) {
	b.owner.ops = append(b.owner.ops, &ops.FileWrite{
		InputID:  b.info.ID,
		FilePath: filePath,
	})
}

func (b *AgentPlanBuilder) ECCompute(ec stgmod.EC, inBlockIndexes []int, outBlockIndexes []int, streams ...*AgentStream) *MultiStream {
	mstr := &MultiStream{}

	var inputStrIDs []ioswitch.StreamID
	for _, str := range streams {
		inputStrIDs = append(inputStrIDs, str.info.ID)
	}

	var outputStrIDs []ioswitch.StreamID
	for i := 0; i < ec.N-ec.K; i++ {
		info := b.owner.newStream()
		mstr.Streams = append(mstr.Streams, &AgentStream{
			owner: b,
			info:  info,
		})
		outputStrIDs = append(outputStrIDs, info.ID)
	}

	b.ops = append(b.ops, &ops.ECCompute{
		EC:                 ec,
		InputIDs:           inputStrIDs,
		OutputIDs:          outputStrIDs,
		InputBlockIndexes:  inBlockIndexes,
		OutputBlockIndexes: outBlockIndexes,
	})

	return mstr
}

func (b *AgentPlanBuilder) ECReconstruct(ec stgmod.EC, inBlockIndexes []int, streams ...*AgentStream) *MultiStream {
	mstr := &MultiStream{}

	var inputStrIDs []ioswitch.StreamID
	for _, str := range streams {
		inputStrIDs = append(inputStrIDs, str.info.ID)
	}

	var outputStrIDs []ioswitch.StreamID
	for i := 0; i < ec.K; i++ {
		info := b.owner.newStream()
		mstr.Streams = append(mstr.Streams, &AgentStream{
			owner: b,
			info:  info,
		})
		outputStrIDs = append(outputStrIDs, info.ID)
	}

	b.ops = append(b.ops, &ops.ECReconstruct{
		EC:                ec,
		InputIDs:          inputStrIDs,
		OutputIDs:         outputStrIDs,
		InputBlockIndexes: inBlockIndexes,
	})

	return mstr
}
