package ops

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	myio "gitlink.org.cn/cloudream/common/utils/io"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type FileWrite struct {
	InputID  ioswitch.StreamID `json:"inputID"`
	FilePath string            `json:"filePath"`
}

func (o *FileWrite) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	str, err := sw.WaitStreams(planID, o.InputID)
	if err != nil {
		return err
	}
	defer str[0].Stream.Close()

	dir := path.Dir(o.FilePath)
	err = os.MkdirAll(dir, 0777)
	if err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}

	file, err := os.Create(o.FilePath)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()

	_, err = io.Copy(file, str[0].Stream)
	if err != nil {
		return fmt.Errorf("copying data to file: %w", err)
	}

	return nil
}

type FileRead struct {
	OutputID ioswitch.StreamID `json:"outputID"`
	FilePath string            `json:"filePath"`
}

func (o *FileRead) Execute(sw *ioswitch.Switch, planID ioswitch.PlanID) error {
	file, err := os.Open(o.FilePath)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}

	fut := future.NewSetVoid()
	sw.StreamReady(planID, ioswitch.NewStream(o.OutputID, myio.AfterReadClosed(file, func(closer io.ReadCloser) {
		fut.SetVoid()
	})))

	fut.Wait(context.TODO())

	return nil
}

func init() {
	OpUnion.AddT((*FileRead)(nil))
	OpUnion.AddT((*FileWrite)(nil))
}
