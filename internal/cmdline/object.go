package cmdline

import (
	"fmt"

	"github.com/jedib0t/go-pretty/v6/table"
	"gitlink.org.cn/cloudream/client/internal/services"
)

func (c *Commandline) GetBucketObjects(bucketID int) error {
	userID := 0

	objects, err := services.BucketSvc(c.svc).GetBucketObjects(userID, bucketID)
	if err != nil {
		return err
	}

	fmt.Printf("Find %d objects in bucket %d for user %d:\n", len(objects), bucketID, userID)

	tb := table.NewWriter()
	tb.AppendHeader(table.Row{"ID", "Name", "Size", "Redundancy", "NumRep", "ECName"})

	for _, obj := range objects {
		tb.AppendRow(table.Row{obj.ObjectID, obj.Name, obj.FileSizeInBytes, obj.Redundancy, obj.NumRep, obj.ECName})
	}

	fmt.Print(tb.Render())
	return nil
}

func (c *Commandline) DeleteObject(objectID int) error {
	userID := 0
	err := services.ObjectSvc(c.svc).DeleteObject(userID, objectID)
	if err != nil {
		return fmt.Errorf("delete object %d failed, err: %w", objectID, err)
	}
	return nil
}
