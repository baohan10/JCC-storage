package ops

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/utils/io2"
	"gitlink.org.cn/cloudream/storage/common/pkgs/ioswitch"
)

type FileWrite struct {
	Input    *ioswitch.StreamVar `json:"input"`
	FilePath string              `json:"filePath"`
}

func (o *FileWrite) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	err := sw.BindVars(ctx, o.Input)
	if err != nil {
		return err
	}
	defer o.Input.Stream.Close()

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

	_, err = io.Copy(file, o.Input.Stream)
	if err != nil {
		return fmt.Errorf("copying data to file: %w", err)
	}

	return nil
}

type FileRead struct {
	Output   *ioswitch.StreamVar `json:"output"`
	FilePath string              `json:"filePath"`
}

func (o *FileRead) Execute(ctx context.Context, sw *ioswitch.Switch) error {
	file, err := os.Open(o.FilePath)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}

	fut := future.NewSetVoid()
	o.Output.Stream = io2.AfterReadClosed(file, func(closer io.ReadCloser) {
		fut.SetVoid()
	})
	sw.PutVars(o.Output)
	fut.Wait(ctx)

	return nil
}

func init() {
	OpUnion.AddT((*FileRead)(nil))
	OpUnion.AddT((*FileWrite)(nil))
}
