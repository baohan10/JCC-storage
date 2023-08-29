package db

type UserBucketDB struct {
	*DB
}

func (db *DB) UserBucket() *UserBucketDB {
	return &UserBucketDB{DB: db}
}

func (*UserBucketDB) Create(ctx SQLContext, userID int64, bucketID int64) error {
	_, err := ctx.Exec("insert into UserBucket(UserID,BucketID) values(?,?)", userID, bucketID)
	return err
}
