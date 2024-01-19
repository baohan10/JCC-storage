package plans

import (
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch/ops"
)

type AgentPlanBuilder struct {
	owner *PlanBuilder
	node  cdssdk.Node
	ops   []ioswitch.Op
}

type AgentStream struct {
	owner *AgentPlanBuilder
	info  *StreamInfo
}

func (b *AgentPlanBuilder) Build(planID ioswitch.PlanID) (AgentPlan, error) {
	plan := ioswitch.Plan{
		ID:  planID,
		Ops: b.ops,
	}

	return AgentPlan{
		Plan: plan,
		Node: b.node,
	}, nil
}

func (b *AgentPlanBuilder) GRCPFetch(node cdssdk.Node, str *AgentStream) *AgentStream {
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

func (s *AgentStream) GRPCSend(node cdssdk.Node) *AgentStream {
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

func (b *AgentPlanBuilder) IPFSRead(fileHash string) *AgentStream {
	agtStr := &AgentStream{
		owner: b,
		info:  b.owner.newStream(),
	}

	b.ops = append(b.ops, &ops.IPFSRead{
		Output:   agtStr.info.ID,
		FileHash: fileHash,
	})

	return agtStr
}

func (s *AgentStream) IPFSWrite(resultKey string) {
	s.owner.ops = append(s.owner.ops, &ops.IPFSWrite{
		Input:     s.info.ID,
		ResultKey: resultKey,
	})
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

func (b *AgentPlanBuilder) ECReconstructAny(ec cdssdk.ECRedundancy, inBlockIndexes []int, outBlockIndexes []int, streams ...*AgentStream) *MultiStream {
	mstr := &MultiStream{}

	var inputStrIDs []ioswitch.StreamID
	for _, str := range streams {
		inputStrIDs = append(inputStrIDs, str.info.ID)
	}

	var outputStrIDs []ioswitch.StreamID
	for i := 0; i < len(outBlockIndexes); i++ {
		info := b.owner.newStream()
		mstr.Streams = append(mstr.Streams, &AgentStream{
			owner: b,
			info:  info,
		})
		outputStrIDs = append(outputStrIDs, info.ID)
	}

	b.ops = append(b.ops, &ops.ECReconstructAny{
		EC:                 ec,
		InputIDs:           inputStrIDs,
		OutputIDs:          outputStrIDs,
		InputBlockIndexes:  inBlockIndexes,
		OutputBlockIndexes: outBlockIndexes,
	})

	return mstr
}

func (b *AgentPlanBuilder) ECReconstruct(ec cdssdk.ECRedundancy, inBlockIndexes []int, streams ...*AgentStream) *MultiStream {
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

func (b *AgentStream) ChunkedSplit(chunkSize int, streamCount int, paddingZeros bool) *MultiStream {
	mstr := &MultiStream{}

	var outputStrIDs []ioswitch.StreamID
	for i := 0; i < streamCount; i++ {
		info := b.owner.owner.newStream()
		mstr.Streams = append(mstr.Streams, &AgentStream{
			owner: b.owner,
			info:  info,
		})
		outputStrIDs = append(outputStrIDs, info.ID)
	}

	b.owner.ops = append(b.owner.ops, &ops.ChunkedSplit{
		InputID:      b.info.ID,
		OutputIDs:    outputStrIDs,
		ChunkSize:    chunkSize,
		StreamCount:  streamCount,
		PaddingZeros: paddingZeros,
	})

	return mstr
}

func (s *AgentStream) Length(length int64) *AgentStream {
	agtStr := &AgentStream{
		owner: s.owner,
		info:  s.owner.owner.newStream(),
	}

	s.owner.ops = append(s.owner.ops, &ops.Length{
		InputID:  s.info.ID,
		OutputID: agtStr.info.ID,
		Length:   length,
	})

	return agtStr
}

func (s *AgentStream) ToExecutor() *ToExecutorStream {
	return &ToExecutorStream{
		info:     s.info,
		fromNode: &s.owner.node,
	}
}

func (b *AgentPlanBuilder) Join(length int64, streams ...*AgentStream) *AgentStream {
	agtStr := &AgentStream{
		owner: b,
		info:  b.owner.newStream(),
	}

	var inputStrIDs []ioswitch.StreamID
	for _, str := range streams {
		inputStrIDs = append(inputStrIDs, str.info.ID)
	}

	b.ops = append(b.ops, &ops.Join{
		InputIDs: inputStrIDs,
		OutputID: agtStr.info.ID,
		Length:   length,
	})

	return agtStr
}

func (b *AgentPlanBuilder) ChunkedJoin(chunkSize int, streams ...*AgentStream) *AgentStream {
	agtStr := &AgentStream{
		owner: b,
		info:  b.owner.newStream(),
	}

	var inputStrIDs []ioswitch.StreamID
	for _, str := range streams {
		inputStrIDs = append(inputStrIDs, str.info.ID)
	}

	b.ops = append(b.ops, &ops.ChunkedJoin{
		InputIDs:  inputStrIDs,
		OutputID:  agtStr.info.ID,
		ChunkSize: chunkSize,
	})

	return agtStr
}
