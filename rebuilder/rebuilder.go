package rebuilder

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/FogMeta/rebuilder-tools/rebuilder/aria2"
	"github.com/FogMeta/rebuilder-tools/rebuilder/config"
	"github.com/FogMeta/rebuilder-tools/rebuilder/log"
	"github.com/FogMeta/rebuilder-tools/rebuilder/lotus"
	"github.com/FogMeta/rebuilder-tools/rebuilder/mcs"
	"github.com/filedrive-team/go-graphsplit"
)

func Init(confPath ...string) (rebuilder *Rebuilder, err error) {
	conf, err := config.Init(confPath...)
	if err != nil {
		return
	}
	return NewRebuilder(conf)
}

func Build(name string, fileURLs ...string) (downloadURL string, err error) {
	rebuilder, err := Init()
	if err != nil {
		return "", err
	}
	return rebuilder.Build(name, fileURLs...)
}

type Rebuilder struct {
	inputPath    string
	outputPath   string
	parallel     int
	bucketClient *mcs.BucketClient
	aria2Client  *aria2.Client
	lotusClient  *lotus.Client
	wallet       string
}

func NewRebuilder(conf *config.Config) (r *Rebuilder, err error) {
	if conf == nil {
		return nil, errors.New("conf not be nil")
	}
	if conf.Task == nil {
		return nil, errors.New("conf not set task")
	}
	parallet := conf.Task.Parallel
	if parallet == 0 {
		parallet = 3
	}

	// init aria2 client
	if conf.Aria2 == nil {
		return nil, errors.New("conf not set aria2")
	}
	aria2Client := aria2.NewClient(conf.Aria2.Host, conf.Aria2.Port, conf.Aria2.Secret)

	// init mcs
	if conf.MCS == nil {
		return nil, errors.New("conf not set mcs")
	}
	bucketClient, err := mcs.NewBucketClient(conf.MCS.APIKey, conf.MCS.APIToken, conf.MCS.Network, conf.MCS.BucketName)
	if err != nil {
		return
	}

	// init lotus
	var lotusClient *lotus.Client
	wallet := ""
	if conf.Lotus != nil {
		wallet = conf.Lotus.Wallet
		lotusClient, err = lotus.NewClient(conf.Lotus.NodeApi, conf.Lotus.Timeout)
		if err != nil {
			return
		}
	} else {
		log.Warn("conf not set lotus")
	}

	return &Rebuilder{
		inputPath:    conf.Task.InputPath,
		outputPath:   conf.Task.OutputPath,
		parallel:     parallet,
		bucketClient: bucketClient,
		aria2Client:  aria2Client,
		lotusClient:  lotusClient,
		wallet:       wallet,
	}, nil
}

// Build builds source file from car file url
func (r *Rebuilder) Build(name string, fileURLs ...string) (downloadURL string, err error) {
	if len(fileURLs) == 0 {
		return "", errors.New("no file URLs")
	}
	if r.inputPath == r.outputPath {
		return "", errors.New("input path not be same with output path")
	}
	if name == "" {
		name = filepath.Base(fileURLs[0])
	}

	sourceDir := filepath.Join(r.outputPath, name)
	if err = os.MkdirAll(sourceDir, 0766); err != nil {
		return
	}

	carDir := filepath.Join(r.inputPath, name)
	if err = os.MkdirAll(carDir, 0766); err != nil {
		return
	}

	log.Info("start download ...")
	//download car file
	downloader := NewDownloader(r.parallel, r.aria2Client)
	_, err = downloader.DownloadFiles(carDir, fileURLs...)
	if err != nil {
		return
	}
	log.Info("download complete, start restore from car ...")
	return r.RestoreAndUpload(carDir, sourceDir)
}

func (r *Rebuilder) RestoreAndUpload(carPath, outputDir string) (downloadURL string, err error) {
	// restore from car
	graphsplit.CarTo(carPath, outputDir, r.parallel)
	graphsplit.Merge(outputDir, r.parallel, true)
	log.Info("restore complete, start upload source file ...")
	// upload file
	err = filepath.WalkDir(outputDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		log.Info("upload file :", path)
		downloadURL, err = r.bucketClient.UploadFile(path, true)
		return err
	})
	return
}

func (r *Rebuilder) Retrieve(name string, carInfos []*CarInfo, wallet string, savePath ...string) (downloadURL string, err error) {
	if len(carInfos) == 0 {
		return "", errors.New("invalid empty carInfos")
	}

	carDir := filepath.Join(r.inputPath, name)
	if err = os.MkdirAll(carDir, 0766); err != nil {
		return
	}
	for _, info := range carInfos {
		if info.CID == "" || len(info.Deals) == 0 {
			return "", errors.New("invalid empty cid or miners")
		}
		success := false
		for _, deal := range info.Deals {
			cid, miner := info.CID, deal.MinerFid
			if err := r.RetrieveFile(cid, miner, wallet, carDir); err == nil {
				log.Infof("retrieve file %s with miner :%s success\n", cid, miner)
				success = true
				break
			} else {
				log.Errorf("retrieve file %s with miner :%s failed: %v\n", cid, miner, err)
			}
		}
		if !success {
			log.Errorf("retrieve file %s with all miners failed: %v\n", info.CID, err)
			return "", fmt.Errorf("retrieve failed with file :%s", info.CID)
		}
	}
	path := r.outputPath
	if len(savePath) > 0 && savePath[0] != "" {
		path = savePath[0]
	}
	sourceDir := filepath.Join(path, name)
	if err = os.MkdirAll(sourceDir, 0766); err != nil {
		return
	}
	return r.RestoreAndUpload(carDir, sourceDir)
}

func (r *Rebuilder) RetrieveFile(cid, miner string, wallet string, savePath string) (err error) {
	if r.lotusClient == nil {
		return errors.New("conf net set lotus")
	}
	if cid == "" || miner == "" {
		return errors.New("invalid empty cid or miner")
	}
	if wallet == "" {
		wallet = r.wallet
	}
	path := filepath.Join(savePath, fmt.Sprintf("%s-%s.car", miner, cid))
	return r.lotusClient.RetrieveData(miner, cid, path, wallet)
}

type CarInfo struct {
	CarFileUrl string     `json:"CarFileUrl"`
	CID        string     `json:"PayloadCid"`
	Deals      []*CarDeal `json:"Deals"`
}

type CarDeal struct {
	DealId   int
	DealCid  string
	MinerFid string
}
