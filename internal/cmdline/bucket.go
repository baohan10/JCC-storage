package cmdline

import (
	"fmt"

	"github.com/jedib0t/go-pretty/v6/table"
	"gitlink.org.cn/cloudream/client/internal/services"
)

func (c *Commandline) GetUserBuckets() error {
	userID := 0

	buckets, err := services.BucketSvc(c.svc).GetUserBuckets(userID)
	if err != nil {
		return err
	}

	fmt.Printf("Find %d buckets for user %d:\n", len(buckets), userID)

	tb := table.NewWriter()
	tb.AppendHeader(table.Row{"ID", "Name", "CreatorID"})

	for _, bucket := range buckets {
		tb.AppendRow(table.Row{bucket.BucketID, bucket.Name, bucket.CreatorID})
	}

	fmt.Print(tb.Render())
	return nil
}

func (c *Commandline) CreateBucket(bucketName string) error {
	userID := 0

	bucketID, err := services.BucketSvc(c.svc).CreateBucket(userID, bucketName)
	if err != nil {
		return err
	}

	fmt.Printf("Create bucket %s success, id: %d", bucketName, bucketID)
	return nil
}
