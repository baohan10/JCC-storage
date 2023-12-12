package ops

import (
	"context"
	"io"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	myio "gitlink.org.cn/cloudream/common/utils/io"
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
			myio.AfterReadClosedOnce(myio.Length(strs[0].Stream, o.Length), func(closer io.ReadCloser) {
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
