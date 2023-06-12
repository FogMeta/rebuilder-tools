package mcs

import (
	"net/url"
	"path/filepath"

	"github.com/filswan/go-mcs-sdk/mcs/api/bucket"
	"github.com/filswan/go-mcs-sdk/mcs/api/common/logs"
	"github.com/filswan/go-mcs-sdk/mcs/api/user"
)

type BucketClient struct {
	client     *bucket.BucketClient
	bucketName string
}

func NewBucketClient(key, token, network, bucketName string) (client *BucketClient, err error) {
	mcsClient, err := user.LoginByApikey(key, token, network)
	if err != nil {
		return
	}
	bucketClient := bucket.GetBucketClient(*mcsClient)
	_, err = bucketClient.GetBucket(bucketName, "")
	if err != nil {
		_, err = bucketClient.CreateBucket(bucketName)
		if err != nil {
			return
		}
	}
	logs.GetLogger().Level = 3
	return &BucketClient{
		client:     bucketClient,
		bucketName: bucketName,
	}, nil
}

func (bc *BucketClient) UploadFile(path string, replace bool) (downloadURL string, err error) {
	name := filepath.Base(path)
	if err = bc.client.UploadFile(bc.bucketName, name, path, replace); err != nil {
		return
	}
	ossFile, err := bc.client.GetFile(bc.bucketName, name)
	if err != nil {
		return
	}
	gateway, err := bc.client.GetGateway()
	if err != nil {
		return
	}
	return url.JoinPath(*gateway, "ipfs", ossFile.PayloadCid)
}
