package ops

import (
	"context"
	"fmt"
	"io"
	"sync"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/logger"
	"gitlink.org.cn/cloudream/common/pkgs/types"
	myio "gitlink.org.cn/cloudream/common/utils/io"
	"gitlink.org.cn/cloudream/common/utils/serder"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/db/model"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

var _ = serder.UseTypeUnionExternallyTagged(types.Ref(types.NewTypeUnion[ioswitch.Op](
	(*IPFSRead)(nil),
	(*IPFSWrite)(nil),
	(*GRPCSend)(nil),
	(*GRPCFetch)(nil),
	(*ECCompute)(nil),
	(*Combine)(nil),
)))

type IPFSRead struct {
	Output   ioswitch.StreamID `json:"output"`
	FileHash string            `json:"fileHash"`
}

func (o *IPFSRead) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	logger.
		WithField("FileHash", o.FileHash).
		WithField("Output", o.Output).
		Debugf("ipfs read op")
	defer logger.Debugf("ipfs read op finished")

	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer stgglb.IPFSPool.Release(ipfsCli)

	file, err := ipfsCli.OpenRead(o.FileHash)
	if err != nil {
		return fmt.Errorf("reading ipfs: %w", err)
	}

	fut := future.NewSetVoid()
	file = myio.AfterReadClosed(file, func(closer io.ReadCloser) {
		fut.SetVoid()
	})

	sw.StreamReady(planID, ioswitch.NewStream(o.Output, file))

	// TODO context
	fut.Wait(context.TODO())
	return nil
}

type IPFSWrite struct {
	Input     ioswitch.StreamID `json:"input"`
	ResultKey string            `json:"resultKey"`
}

func (o *IPFSWrite) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	logger.
		WithField("ResultKey", o.ResultKey).
		WithField("Input", o.Input).
		Debugf("ipfs write op")

	ipfsCli, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		return fmt.Errorf("new ipfs client: %w", err)
	}
	defer stgglb.IPFSPool.Release(ipfsCli)

	strs, err := sw.WaitStreams(planID, o.Input)
	if err != nil {
		return err
	}
	defer strs[0].Stream.Close()

	fileHash, err := ipfsCli.CreateFile(strs[0].Stream)
	if err != nil {
		return fmt.Errorf("creating ipfs file: %w", err)
	}

	if o.ResultKey != "" {
		sw.AddResultValue(planID, ioswitch.ResultKV{
			Key:   o.ResultKey,
			Value: fileHash,
		})
	}

	return nil
}

type GRPCSend struct {
	StreamID ioswitch.StreamID `json:"streamID"`
	Node     model.Node        `json:"node"`
}

func (o *GRPCSend) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	logger.
		WithField("ioswitch.StreamID", o.StreamID).
		Debugf("grpc send")

	strs, err := sw.WaitStreams(planID, o.StreamID)
	if err != nil {
		return err
	}
	defer strs[0].Stream.Close()

	// TODO 根据客户端地址选择IP和端口
	agtCli, err := stgglb.AgentRPCPool.Acquire(o.Node.ExternalIP, o.Node.ExternalGRPCPort)
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	err = agtCli.SendStream(planID, o.StreamID, strs[0].Stream)
	if err != nil {
		return fmt.Errorf("sending stream: %w", err)
	}

	return nil
}

type GRPCFetch struct {
	StreamID ioswitch.StreamID `json:"streamID"`
	Node     model.Node        `json:"node"`
}

func (o *GRPCFetch) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	// TODO 根据客户端地址选择IP和端口
	agtCli, err := stgglb.AgentRPCPool.Acquire(o.Node.ExternalIP, o.Node.ExternalGRPCPort)
	if err != nil {
		return fmt.Errorf("new agent rpc client: %w", err)
	}
	defer stgglb.AgentRPCPool.Release(agtCli)

	str, err := agtCli.FetchStream(planID, o.StreamID)
	if err != nil {
		return fmt.Errorf("fetching stream: %w", err)
	}

	fut := future.NewSetVoid()
	str = myio.AfterReadClosed(str, func(closer io.ReadCloser) {
		fut.SetVoid()
	})

	sw.StreamReady(planID, ioswitch.NewStream(o.StreamID, str))

	// TODO
	fut.Wait(context.TODO())

	return err
}

type ECCompute struct {
	EC                 stgmod.EC           `json:"ec"`
	InputIDs           []ioswitch.StreamID `json:"inputIDs"`
	OutputIDs          []ioswitch.StreamID `json:"outputIDs"`
	InputBlockIndexes  []int               `json:"inputBlockIndexes"`
	OutputBlockIndexes []int               `json:"outputBlockIndexes"`
}

func (o *ECCompute) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	rs, err := ec.NewRs(o.EC.K, o.EC.N, o.EC.ChunkSize)
	if err != nil {
		return fmt.Errorf("new ec: %w", err)
	}

	strs, err := sw.WaitStreams(planID, o.InputIDs...)
	if err != nil {
		return err
	}
	defer func() {
		for _, s := range strs {
			s.Stream.Close()
		}
	}()

	var inputs []io.ReadCloser
	for _, s := range strs {
		inputs = append(inputs, s.Stream)
	}

	outputs, err := rs.ReconstructSome(inputs, o.InputBlockIndexes, o.OutputBlockIndexes)
	if err != nil {
		return fmt.Errorf("reconstructing: %w", err)
	}

	wg := sync.WaitGroup{}
	for i, id := range o.OutputIDs {
		wg.Add(1)
		sw.StreamReady(planID, ioswitch.NewStream(id, myio.AfterReadClosed(outputs[i], func(closer io.ReadCloser) {
			wg.Done()
		})))
	}
	wg.Wait()

	return nil
}

type Combine struct {
	InputIDs []ioswitch.StreamID `json:"inputIDs"`
	OutputID ioswitch.StreamID   `json:"outputID"`
}

func (o *Combine) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	strs, err := sw.WaitStreams(planID, o.InputIDs...)
	if err != nil {
		return err
	}

	pr, pw := io.Pipe()
	sw.StreamReady(planID, ioswitch.NewStream(o.OutputID, pr))

	for _, str := range strs {
		_, err := io.Copy(pw, str.Stream)
		if err != nil {
			return err
		}
	}

	return nil
}
