package ops

import (
	"context"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type Length struct {
	InputID  ioswitch.StreamID `json:"inputID"`
	OutputID ioswitch.StreamID `json:"outputID"`
	Length   int64             `json:"length"`
}

func (o *Length) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	strs, err := sw.WaitStreams(planID, o.InputID)
	if err != nil {
		return err
	}
	defer strs[0].Stream.Close()

	fut := future.NewSetVoid()
	sw.StreamReady(planID,
		ioswitch.NewStream(o.OutputID,
			io2.AfterReadClosedOnce(io2.Length(strs[0].Stream, o.Length), func(closer io.ReadCloser) {
				fut.SetVoid()
			}),
		),
	)

	fut.Wait(context.TODO())
	return nil
}

func init() {
	OpUnion.AddT((*Length)(nil))
}
