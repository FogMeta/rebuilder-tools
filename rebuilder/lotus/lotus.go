package lotus

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/FogMeta/rebuilder-tools/rebuilder/log"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
)

type Client struct {
	node   api.FullNode
	closer jsonrpc.ClientCloser
}

func NewClient(fullNodeApi string, timeout ...int) (c *Client, err error) {
	var ctx context.Context
	if len(timeout) > 0 && timeout[0] > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), time.Second*time.Duration(timeout[0]))
		defer cancel()
	} else {
		ctx = context.Background()
	}

	api := ParseApiInfo(fullNodeApi)
	addr, err := api.DialArgs("v1")
	if err != nil {
		log.Errorf("init Client could not get DialArgs: %w", err)
		return
	}
	fullNode, closer, err := client.NewFullNodeRPCV1(ctx, addr, api.AuthHeader())
	if err != nil {
		if closer != nil {
			closer()
		}
		log.Errorf("init Client could not get DialArgs: %v", err)
		return
	}
	return &Client{
		node:   fullNode,
		closer: closer,
	}, nil
}

func SaveMinerByBrowser() {
	for page := 0; page < 500; page++ {
		log.Infof("page=%d", page)
		miners, err := getMiner(page)
		time.Sleep(3 * time.Second)
		if err != nil {
			continue
		}
		if len(miners) < 50 {
			break
		}
	}
}

func getMiner(page int) (miners []string, err error) {
	resp, err := http.Get(fmt.Sprintf("https://filfox.info/api/v1/miner/list/power?pageSize=50&page=%d", page))
	if err != nil {
		log.Errorf("get resp error: %v", err)
		return
	}
	defer resp.Body.Close()
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}
	var minerData MinerData
	if err = json.Unmarshal(bytes, &minerData); err != nil {
		log.Errorf("resp to json error: %v", err)
		return
	}

	if len(minerData.Miners) == 0 {
		return
	}

	for _, miner := range minerData.Miners {
		miners = append(miners, miner.Address)
	}
	return
}

func (lotus *Client) GetMinerInfoByFId(minerId string) (string, error) {
	defer lotus.closer()
	addr, _ := address.NewFromString(minerId)
	minerInfo, err := lotus.node.StateMinerInfo(context.TODO(), addr, types.EmptyTSK)
	if err != nil {
		log.Errorf("get minerInfo failed, minerId: %s,error: %v", addr.String(), err)
		return "", err
	}
	if minerInfo.PeerId == nil {
		return "", fmt.Errorf("minerId:[%s],peerId is nil", addr.String())
	}
	return minerInfo.PeerId.String(), nil
}

func (lotus *Client) ListMiners() ([]address.Address, error) {
	defer lotus.closer()
	return lotus.node.StateListMiners(context.TODO(), types.EmptyTSK)
}

func (lotus *Client) getDealsCounts() (map[address.Address]int, error) {
	defer lotus.closer()
	allDeals, err := lotus.node.StateMarketDeals(context.TODO(), types.EmptyTSK)
	if err != nil {
		return nil, err
	}
	out := make(map[address.Address]int)
	for _, d := range allDeals {
		if d.State.SectorStartEpoch != -1 {
			out[d.Proposal.Provider]++
		}
	}
	return out, nil
}

func (lotus *Client) RetrieveData(minerId, dataCid, savePath, wallet string) error {
	defer lotus.closer()
	log.Infof("start retrieve-data from minerId: %s,datacid: %s,savepath:%s", minerId, dataCid, savePath)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	addr, err := address.NewFromString(minerId)
	if err != nil {
		log.Errorf("init address failed, minerId: %s,error: %v", minerId, err)
		return err
	}

	root, err := cid.Parse(dataCid)
	if err != nil {
		log.Errorf("parse cid failed , dataCid: %s,error: %v", dataCid, err)
		return err
	}
	offer, err := lotus.node.ClientMinerQueryOffer(context.TODO(), addr, root, nil)
	if err != nil {
		return err
	}

	var sel *api.Selector
	// wallet address
	pay, err := address.NewFromString(wallet)
	if err != nil {
		return err
	}
	o := offer.Order(pay)
	o.DataSelector = sel

	subscribeEvents, err := lotus.node.ClientGetRetrievalUpdates(context.TODO())
	if err != nil {
		return fmt.Errorf("error setting up retrieval updates: %w", err)
	}

	retrievalRes, err := lotus.node.ClientRetrieve(context.TODO(), o)
	if err != nil {
		return fmt.Errorf("error setting up retrieval: %w", err)
	}

	start := time.Now()
readEvents:
	for {
		var evt api.RetrievalInfo
		select {
		case <-ctx.Done():
			return errors.New("retrieval timeout")
		case evt = <-subscribeEvents:
			if evt.ID != retrievalRes.DealID {
				continue
			}
		}

		event := "New"
		if evt.Event != nil {
			event = retrievalmarket.ClientEvents[*evt.Event]
		}

		log.Infof("Recv %s, Paid %s, %s (%s), %s\n",
			types.SizeStr(types.NewInt(evt.BytesReceived)),
			types.FIL(evt.TotalPaid),
			strings.TrimPrefix(event, "ClientEvent"),
			strings.TrimPrefix(retrievalmarket.DealStatuses[evt.Status], "DealStatus"),
			time.Since(start).Truncate(time.Millisecond),
		)

		switch evt.Status {
		case retrievalmarket.DealStatusCompleted:
			break readEvents
		case retrievalmarket.DealStatusRejected:
			return fmt.Errorf("retrieval Proposal Rejected: %s", evt.Message)
		case
			retrievalmarket.DealStatusDealNotFound,
			retrievalmarket.DealStatusErrored:
			return fmt.Errorf("retrieval error: %s", evt.Message)
		}
	}

	return lotus.node.ClientExport(ctx, api.ExportRef{
		Root:   root,
		DealID: retrievalRes.DealID,
	}, api.FileRef{
		Path:  savePath,
		IsCAR: true,
	})
}

func (lotus *Client) GetCurrentHeight() (int64, error) {
	defer lotus.closer()
	tipSet, err := lotus.node.ChainHead(context.TODO())
	if err != nil {
		log.Errorf("get ChainHead failed,error: %v", err)
		return 0, err
	}
	return int64(tipSet.Height()), nil
}

func (lotus *Client) Close() {
}

func ArchiveDir(src, out string) error {
	saveFile, err := os.Create(out)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(saveFile)
	defer tw.Close()
	err = filepath.Walk(src, func(file string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		header, err := tar.FileInfoHeader(fi, file)
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(file)
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if !fi.IsDir() {
			data, err := os.Open(file)
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, data); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

type MinerData struct {
	Miners []struct {
		Address string `json:"address"`
	} `json:"miners"`
}
