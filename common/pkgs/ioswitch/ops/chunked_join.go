package ops

import (
	"context"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type ChunkedJoin struct {
	InputIDs  []ioswitch.StreamID `json:"inputIDs"`
	OutputID  ioswitch.StreamID   `json:"outputID"`
	ChunkSize int                 `json:"chunkSize"`
}

func (o *ChunkedJoin) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	strs, err := sw.WaitStreams(planID, o.InputIDs...)
	if err != nil {
		return err
	}

	var strReaders []io.Reader
	for _, s := range strs {
		strReaders = append(strReaders, s.Stream)
	}
	defer func() {
		for _, str := range strs {
			str.Stream.Close()
		}
	}()

	fut := future.NewSetVoid()
	sw.StreamReady(planID,
		ioswitch.NewStream(o.OutputID,
			io2.AfterReadClosedOnce(io2.ChunkedJoin(strReaders, o.ChunkSize), func(closer io.ReadCloser) {
				fut.SetVoid()
			}),
		),
	)

	fut.Wait(context.TODO())
	return nil
}

func init() {
	OpUnion.AddT((*ChunkedJoin)(nil))
}
