package ops

import (
	"io"
	"sync"

	myio "gitlink.org.cn/cloudream/common/utils/io"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type Clone struct {
	InputID   ioswitch.StreamID   `json:"inputID"`
	OutputIDs []ioswitch.StreamID `json:"outputIDs"`
}

func (o *Clone) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	strs, err := sw.WaitStreams(planID, o.InputID)
	if err != nil {
		return err
	}
	defer strs[0].Stream.Close()

	wg := sync.WaitGroup{}
	cloned := myio.Clone(strs[0].Stream, len(o.OutputIDs))
	for i, s := range cloned {
		wg.Add(1)

		sw.StreamReady(planID,
			ioswitch.NewStream(o.OutputIDs[i],
				myio.AfterReadClosedOnce(s, func(closer io.ReadCloser) {
					wg.Done()
				}),
			),
		)
	}

	wg.Wait()
	return nil
}

func init() {
	OpUnion.AddT((*Clone)(nil))
}
