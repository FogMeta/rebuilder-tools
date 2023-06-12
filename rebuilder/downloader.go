package rebuilder

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/FogMeta/rebuilder-tools/rebuilder/aria2"
	"github.com/FogMeta/rebuilder-tools/rebuilder/log"
)

type DownloadInfo struct {
	DirPath string
	FileURL string
	Gid     string
	Status  int
	Err     error
}

type Downloader struct {
	total       int
	maxNum      int
	preChan     chan *DownloadInfo
	inChan      chan *DownloadInfo
	exit        chan bool
	statusMap   map[string]*DownloadInfo
	aria2Client *aria2.Client
}

func NewDownloader(max int, aria2Client *aria2.Client) *Downloader {
	return &Downloader{
		maxNum:      max,
		preChan:     make(chan *DownloadInfo, max),
		inChan:      make(chan *DownloadInfo, max),
		exit:        make(chan bool),
		aria2Client: aria2Client,
	}
}

func (downloader *Downloader) DownloadFiles(dirPath string, fileURLs ...string) (status map[string]*DownloadInfo, err error) {
	info, err := os.Stat(dirPath)
	if err != nil {
		return
	}
	if !info.IsDir() {
		return nil, errors.New("dir path is not a directory")
	}
	downloader.total = len(fileURLs)
	downloader.statusMap = make(map[string]*DownloadInfo, downloader.total)
	for _, fileURL := range fileURLs {
		info := &DownloadInfo{
			DirPath: dirPath,
			FileURL: fileURL,
		}
		downloader.statusMap[fileURL] = info
	}

	go func(dirPath string, fileURLs ...string) {
		i := 0
		for {
			if i >= len(fileURLs) {
				log.Info("send download finished")
				return
			}
			fileURL := fileURLs[i]
			select {
			case <-downloader.exit:
				return
			case downloader.preChan <- downloader.statusMap[fileURL]:
				i++
			}
		}
	}(dirPath, fileURLs...)
	err = downloader.checkDownload()
	return downloader.statusMap, err
}

func (downloader *Downloader) checkDownload() error {
	ticker := time.NewTicker(1 * time.Second)
	var temp []*DownloadInfo
	finishChan := make(chan bool)
	go func() {
		finishChan <- true // start the download
	}()
	exit := false
	defer func() {
		if !exit {
			close(downloader.exit)
		}
	}()
	errExit := errors.New("exit")
	for {
		select {
		case <-finishChan:
			if downloader.total == 0 {
				log.Info("download finished")
				return nil
			}
			info := <-downloader.preChan
			log.Info("start download job :", info.FileURL)
			info.Gid, info.Err = downloader.downloadFile(info.DirPath, info.FileURL, ".car")
			if info.Err != nil {
				return info.Err
			}
			log.Info("download gid :", info.Gid)
			downloader.inChan <- info
		case info := <-downloader.inChan:
			ok, err := downloader.downloadStatus(info.Gid)
			if !ok && err == nil {
				temp = append(temp, info)
				break
			}
			if err != nil {
				info.Status = -1
				info.Err = err
				return err
			}
			info.Status = 1
			downloader.total--
			go func() {
				finishChan <- true
			}()
		case <-ticker.C:
			log.Info("ticker query number: ", len(temp))
			for _, info := range temp {
				downloader.inChan <- info
			}
			temp = temp[:0]
		case <-downloader.exit:
			exit = true
			return errExit
		}
	}
}

func (downloader *Downloader) downloadFile(dirPath string, fileURL string, format ...string) (gid string, err error) {
	name := filepath.Base(fileURL)
	if len(format) > 0 {
		name += format[0]
	}
	return downloader.aria2Client.DownloadFile(fileURL, dirPath, name)
}

func (downloader *Downloader) downloadStatus(gid string) (ok bool, err error) {
	return downloader.aria2Client.DownloadStatus(gid)
}
