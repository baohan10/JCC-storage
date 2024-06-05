package downloader

import (
	stgglb "gitlink.org.cn/cloudream/storage/common/globals"
	coormq "gitlink.org.cn/cloudream/storage/common/pkgs/mq/coordinator"
)

type PinConfig struct {
	IpfsPin IpfsConfig
}

type IpfsConfig struct {
	IpfsUrl string `json:"ipfsUrl"`
}

// PinFileToIPFS pin file to IPFS
func PinFileToIPFS(objDetails coormq.GetObjectDetailsResp) error {

	client, err := stgglb.IPFSPool.Acquire()
	if err != nil {
		print(err)
	}

	objs := objDetails.Objects
	// 遍历objDetails
	for _, obj := range objs {
		cid := obj.Object.FileHash
		err := client.Pin(cid)
		if err != nil {
			return err
		}
		print("IPFS pin success, filehash: " + cid)
	}

	return nil
}
