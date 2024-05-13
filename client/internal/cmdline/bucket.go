package cmdline

import (
	"fmt"

	"github.com/jedib0t/go-pretty/v6/table"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
)

func BucketListUserBuckets(ctx CommandContext) error {
	userID := cdssdk.UserID(1)

	buckets, err := ctx.Cmdline.Svc.BucketSvc().GetUserBuckets(userID)
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

func BucketCreateBucket(ctx CommandContext, bucketName string) error {
	userID := cdssdk.UserID(1)

	bucketID, err := ctx.Cmdline.Svc.BucketSvc().CreateBucket(userID, bucketName)
	if err != nil {
		return err
	}

	fmt.Printf("Create bucket %v success, id: %v", bucketName, bucketID)
	return nil
}

func BucketDeleteBucket(ctx CommandContext, bucketID cdssdk.BucketID) error {
	userID := cdssdk.UserID(1)

	err := ctx.Cmdline.Svc.BucketSvc().DeleteBucket(userID, bucketID)
	if err != nil {
		return err
	}

	fmt.Printf("Delete bucket %d success ", bucketID)
	return nil
}

func init() {
	commands.MustAdd(BucketListUserBuckets, "bucket", "ls")

	commands.MustAdd(BucketCreateBucket, "bucket", "new")

	commands.MustAdd(BucketDeleteBucket, "bucket", "delete")
}
