package ops

import (
	"fmt"
	"io"
	"sync"

	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ec"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type ECReconstructAny struct {
	EC                 cdssdk.ECRedundancy `json:"ec"`
	InputIDs           []ioswitch.StreamID `json:"inputIDs"`
	OutputIDs          []ioswitch.StreamID `json:"outputIDs"`
	InputBlockIndexes  []int               `json:"inputBlockIndexes"`
	OutputBlockIndexes []int               `json:"outputBlockIndexes"`
}

func (o *ECReconstructAny) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	rs, err := ec.NewStreamRs(o.EC.K, o.EC.N, o.EC.ChunkSize)
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

	var inputs []io.Reader
	for _, s := range strs {
		inputs = append(inputs, s.Stream)
	}

	outputs := rs.ReconstructAny(inputs, o.InputBlockIndexes, o.OutputBlockIndexes)

	wg := sync.WaitGroup{}
	for i, id := range o.OutputIDs {
		wg.Add(1)
		sw.StreamReady(planID, ioswitch.NewStream(id, io2.AfterReadClosedOnce(outputs[i], func(closer io.ReadCloser) {
			wg.Done()
		})))
	}
	wg.Wait()

	return nil
}

type ECReconstruct struct {
	EC                cdssdk.ECRedundancy `json:"ec"`
	InputIDs          []ioswitch.StreamID `json:"inputIDs"`
	OutputIDs         []ioswitch.StreamID `json:"outputIDs"`
	InputBlockIndexes []int               `json:"inputBlockIndexes"`
}

func (o *ECReconstruct) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	rs, err := ec.NewStreamRs(o.EC.K, o.EC.N, o.EC.ChunkSize)
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

	var inputs []io.Reader
	for _, s := range strs {
		inputs = append(inputs, s.Stream)
	}

	outputs := rs.ReconstructData(inputs, o.InputBlockIndexes)

	wg := sync.WaitGroup{}
	for i, id := range o.OutputIDs {
		wg.Add(1)
		sw.StreamReady(planID, ioswitch.NewStream(id, io2.AfterReadClosedOnce(outputs[i], func(closer io.ReadCloser) {
			wg.Done()
		})))
	}
	wg.Wait()

	return nil
}

func init() {
	OpUnion.AddT((*ECReconstructAny)(nil))
	OpUnion.AddT((*ECReconstruct)(nil))
}
