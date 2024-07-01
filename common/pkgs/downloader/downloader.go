package downloader

import (
	"fmt"
	"io"

	lru "github.com/hashicorp/golang-lru/v2"
	"gitlink.org.cn/cloudream/common/pkgs/iterator"
	cdssdk "gitlink.org.cn/cloudream/common/sdks/storage"
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	stgmod "gitlink.org.cn/cloudream/storage/common/models"
	"gitlink.org.cn/cloudream/storage/common/pkgs/connectivity"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

const (
	DefaultMaxStripCacheCount = 128
)

type DownloadIterator = iterator.Iterator[*Downloading]

type DownloadReqeust struct {
	ObjectID cdssdk.ObjectID
	Offset   int64
	Length   int64
}

type downloadReqeust2 struct {
	Detail *stgmod.ObjectDetail
	Raw    DownloadReqeust
}

type Downloading struct {
	Object  *cdssdk.Object
	File    io.ReadCloser // 文件流，如果文件不存在，那么为nil
	Request DownloadReqeust
}

type Downloader struct {
	strips *StripCache
	conn   *connectivity.Collector
	cfg    Config
}

func NewDownloader(cfg Config, conn *connectivity.Collector) Downloader {
	if cfg.MaxStripCacheCount == 0 {
		cfg.MaxStripCacheCount = DefaultMaxStripCacheCount
	}

	ch, _ := lru.New[ECStripKey, ObjectECStrip](cfg.MaxStripCacheCount)
	return Downloader{
		strips: ch,
		conn:   conn,
		cfg:    cfg,
	}
}

func (d *Downloader) DownloadObjects(reqs []DownloadReqeust) DownloadIterator {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return iterator.FuseError[*Downloading](fmt.Errorf("new coordinator client: %w", err))
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	objIDs := make([]cdssdk.ObjectID, len(reqs))
	for i, req := range reqs {
		objIDs[i] = req.ObjectID
	}

	if len(objIDs) == 0 {
		return iterator.Empty[*Downloading]()
	}

	objDetails, err := coorCli.GetObjectDetails(coormq.ReqGetObjectDetails(objIDs))
	if err != nil {
		return iterator.FuseError[*Downloading](fmt.Errorf("request to coordinator: %w", err))
	}

	// 测试使用：将文件pin到ipfs
	go func() {
		err := PinFileToIPFS(*objDetails)
		if err != nil {
			print(err)
		}
	}()

	req2s := make([]downloadReqeust2, len(reqs))
	for i, req := range reqs {
		req2s[i] = downloadReqeust2{
			Detail: objDetails.Objects[i],
			Raw:    req,
		}
	}

	return NewDownloadObjectIterator(d, req2s)
}

func (d *Downloader) DownloadPackage(pkgID cdssdk.PackageID) DownloadIterator {
	coorCli, err := stgglb.CoordinatorMQPool.Acquire()
	if err != nil {
		return iterator.FuseError[*Downloading](fmt.Errorf("new coordinator client: %w", err))
	}
	defer stgglb.CoordinatorMQPool.Release(coorCli)

	pkgDetail, err := coorCli.GetPackageObjectDetails(coormq.ReqGetPackageObjectDetails(pkgID))
	if err != nil {
		return iterator.FuseError[*Downloading](fmt.Errorf("request to coordinator: %w", err))
	}

	req2s := make([]downloadReqeust2, len(pkgDetail.Objects))
	for i, objDetail := range pkgDetail.Objects {
		dt := objDetail
		req2s[i] = downloadReqeust2{
			Detail: &dt,
			Raw: DownloadReqeust{
				ObjectID: objDetail.Object.ObjectID,
				Offset:   0,
				Length:   objDetail.Object.Size,
			},
		}
	}

	return NewDownloadObjectIterator(d, req2s)
}

type ObjectECStrip struct {
	Data           []byte
	ObjectFileHash string // 添加这条缓存时，Object的FileHash
}

type ECStripKey struct {
	ObjectID   cdssdk.ObjectID
	StripIndex int64
}

type StripCache = lru.Cache[ECStripKey, ObjectECStrip]
