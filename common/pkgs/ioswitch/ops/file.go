package ops

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"gitlink.org.cn/cloudream/common/pkgs/future"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/dag"
	"gitlink.org.cn/cloudream/common/pkgs/ioswitch/exec"
	"gitlink.org.cn/cloudream/common/utils/io2"
)

func init() {
	OpUnion.AddT((*FileRead)(nil))
	OpUnion.AddT((*FileWrite)(nil))
}

type FileWrite struct {
	Input    *exec.StreamVar `json:"input"`
	FilePath string          `json:"filePath"`
}

func (o *FileWrite) Execute(ctx context.Context, e *exec.Executor) error {
	err := e.BindVars(ctx, o.Input)
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
	Output   *exec.StreamVar `json:"output"`
	FilePath string          `json:"filePath"`
}

func (o *FileRead) Execute(ctx context.Context, e *exec.Executor) error {
	file, err := os.Open(o.FilePath)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}

	fut := future.NewSetVoid()
	o.Output.Stream = io2.AfterReadClosed(file, func(closer io.ReadCloser) {
		fut.SetVoid()
	})
	e.PutVars(o.Output)
	fut.Wait(ctx)

	return nil
}

type FileReadType struct {
	FilePath string
}

func (t *FileReadType) InitNode(node *Node) {
	dag.NodeNewOutputStream(node, VarProps{})
}

func (t *FileReadType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&FileRead{
		Output:   op.OutputStreams[0].Props.Var.(*exec.StreamVar),
		FilePath: t.FilePath,
	}, op.Env, blder)
	return nil
}

func (t *FileReadType) String(node *Node) string {
	return fmt.Sprintf("FileRead[%s]%v%v", t.FilePath, formatStreamIO(node), formatValueIO(node))
}

type FileWriteType struct {
	FilePath string
}

func (t *FileWriteType) InitNode(node *Node) {
	dag.NodeDeclareInputStream(node, 1)
}

func (t *FileWriteType) GenerateOp(op *Node, blder *exec.PlanBuilder) error {
	addOpByEnv(&FileWrite{
		Input:    op.InputStreams[0].Props.Var.(*exec.StreamVar),
		FilePath: t.FilePath,
	}, op.Env, blder)
	return nil
}
