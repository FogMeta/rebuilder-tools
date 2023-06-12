package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/FogMeta/rebuilder-tools/rebuilder"
	"github.com/FogMeta/rebuilder-tools/rebuilder/config"
	"github.com/FogMeta/rebuilder-tools/rebuilder/log"
	"github.com/urfave/cli/v2"
)

const (
	defaultConfName = "rebuilder.conf"
	defaultConfPath = "./rebuilder.conf"
)

func main() {
	log.Init()
	app := &cli.App{
		Name:     "rebuilder",
		Flags:    []cli.Flag{},
		Commands: []*cli.Command{initCmd, buildCmd, retrieveCmd},
		Usage:    "A tool to rebuild file",
	}

	if err := app.Run(os.Args); err != nil {
		log.Error(err)
		os.Exit(1)
	}
}

var initCmd = &cli.Command{
	Name:  "init",
	Usage: "init conf file",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "path",
			Usage: "conf save path",
		},
	},
	Action: func(ctx *cli.Context) (err error) {
		path := ctx.String("path")
		if path == "" {
			path = defaultConfPath
		} else {
			path = path + defaultConfName
		}
		conf := config.Config{
			Aria2: new(config.Aria2),
			Task:  new(config.Task),
			MCS:   new(config.MCS),
			Lotus: new(config.Lotus),
		}
		b, _ := tomlMarshal(conf)
		if err = os.WriteFile(path, b, 0666); err != nil {
			return
		}
		log.Info("config file saved to ", path)
		return
	},
}

var buildCmd = &cli.Command{
	Name:  "build",
	Usage: "build new file download url",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "file",
			Aliases: []string{"f"},
			Usage:   "car file metadata json/csv file",
		},
		&cli.StringFlag{
			Name:  "conf",
			Usage: "conf file path",
		},
		&cli.StringFlag{
			Name:  "name",
			Usage: "file dir name in input path",
		},
		&cli.StringFlag{
			Name:  "lotus-node",
			Usage: "lotus node api",
		},
		&cli.StringFlag{
			Name:  "wallet",
			Usage: "wallet address",
		},
		&cli.Int64Flag{
			Name:  "timeout",
			Usage: "timeout in seconds",
		},
		&cli.StringFlag{
			Name:  "save-path",
			Usage: "retrieved file save directory",
		},
	},
	Action: func(ctx *cli.Context) (err error) {
		confPath := ctx.String("conf")
		if confPath == "" {
			confPath = defaultConfPath
		}
		if confPath == "" {
			return errors.New("need run init before build")
		}
		filePath := ctx.String("file")
		carURLs := ctx.Args().Slice()
		if filePath == "" && len(carURLs) == 0 {
			return errors.New("file or download urls is required")
		}
		var carInfos []*rebuilder.CarInfo
		if filePath != "" {
			carInfos, err = readCarFile(filePath)
			if err != nil {
				return err
			}
			for _, info := range carInfos {
				if info.CarFileUrl != "" {
					carURLs = append(carURLs, info.CarFileUrl)
				}
			}
		}
		if len(carURLs) == 0 {
			return errors.New("no valid car urls")
		}
		log.Info("rebuild start ...")
		// init rebuilder
		conf, err := config.Init(confPath)
		if err != nil {
			return err
		}
		lotusNode := ctx.String("lotus-node")
		timeout := ctx.Int("timeout")
		if lotusNode != "" {
			conf.Lotus.NodeApi = lotusNode
		}
		if timeout > 0 {
			conf.Lotus.Timeout = timeout
		}
		// init rebuilder
		rebuilder, err := rebuilder.NewRebuilder(conf)
		if err != nil {
			return err
		}
		fileURL, err := rebuilder.Build(ctx.String("name"), carURLs...)
		if err != nil {
			log.Info("build from car url failed", err)
			if len(carInfos) > 0 && len(carInfos[0].Deals) > 0 {
				log.Info("try retrieve from deal")
				name := ctx.String("name")
				if name == "" {
					name = filepath.Base(filePath)
				}
				fileURL, err = rebuilder.Retrieve(name, carInfos, ctx.String("wallet"))
				if err != nil {
					return
				}
				log.Info("rebuild file success, download url :", fileURL)
				return
			}
			return err
		}
		log.Info("rebuild file success, download url :", fileURL)
		return nil
	},
}

var retrieveCmd = &cli.Command{
	Name:  "retrieve",
	Usage: "retrieve file from filecoin",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "file",
			Aliases: []string{"f"},
			Usage:   "car file metadata json/csv file",
		},
		&cli.StringSliceFlag{
			Name:  "miners",
			Usage: "miner ids, size must be match with cids",
		},
		&cli.StringSliceFlag{
			Name:  "cids",
			Usage: "file payload cids, size must be match with miners",
		},
		&cli.StringFlag{
			Name:  "name",
			Usage: "file dir name in input path",
		},
		&cli.StringFlag{
			Name:  "save-path",
			Usage: "retrieved file save directory",
		},
		&cli.StringFlag{
			Name:  "lotus-node",
			Usage: "lotus node api",
		},
		&cli.StringFlag{
			Name:  "wallet",
			Usage: "wallet address",
		},
		&cli.Int64Flag{
			Name:  "timeout",
			Usage: "timeout in seconds",
		},
		&cli.StringFlag{
			Name:  "conf",
			Usage: "conf file path",
		},
	},
	Action: func(ctx *cli.Context) (err error) {
		var carInfos []*rebuilder.CarInfo
		if filePath := ctx.String("file"); filePath != "" {
			// read from file
			carInfos, err = readCarFile(filePath)
			if err != nil {
				return err
			}
		} else if len(ctx.StringSlice("cids")) > 0 && len(ctx.StringSlice("miners")) > 0 {
			// read form para
			cids := ctx.StringSlice("cids")
			miners := ctx.StringSlice("miners")
			if len(cids) != len(miners) {
				return errors.New("miners size must be equal to miners size")
			}
			if len(cids) == 0 || len(miners) == 0 {
				return errors.New("cids/miners not be empty")
			}
			for i, cid := range cids {
				carInfos = append(carInfos, &rebuilder.CarInfo{
					CID: cid,
					Deals: []*rebuilder.CarDeal{
						{
							MinerFid: miners[i],
						},
					},
				})
			}
		} else {
			return errors.New("file or cids/miners is required")
		}

		// check info
		if len(carInfos) == 0 {
			return errors.New("not found valid info")
		}
		for _, info := range carInfos {
			if info.CID == "" || len(info.Deals) == 0 {
				return errors.New("cids/miners not be empty")
			}
		}

		confPath := ctx.String("conf")
		if confPath == "" {
			confPath = defaultConfPath
		}
		if confPath == "" {
			return errors.New("need run init before build")
		}
		conf, err := config.Init(confPath)
		if err != nil {
			return err
		}

		// check wallet
		if conf.Lotus.Wallet == "" && ctx.String("wallet") == "" {
			return errors.New("wallet is required")
		}
		lotusNode := ctx.String("lotus-node")
		timeout := ctx.Int("timeout")
		if lotusNode != "" {
			conf.Lotus.NodeApi = lotusNode
		}
		if timeout > 0 {
			conf.Lotus.Timeout = timeout
		}
		// init rebuilder
		builder, err := rebuilder.NewRebuilder(conf)
		if err != nil {
			return err
		}

		name := ctx.String("name")
		if name == "" {
			name = carInfos[0].CID
		}

		downloadURL, err := builder.Retrieve(name, carInfos, ctx.String("wallet"), ctx.String("save-path"))
		if err != nil {
			return err
		}
		log.Info("retrieve success, file download url: ", downloadURL)
		return nil
	},
}

func httpDownloadURL(url string) bool {
	return strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://")
}

func readCarFile(path string) (carInfos []*rebuilder.CarInfo, err error) {
	format := filepath.Ext(path)
	if strings.EqualFold(format, ".csv") {
		return readCarCsv(path)
	} else if strings.EqualFold(format, ".json") {
		return readCarJson(path)
	}
	return nil, errors.New("not supported file format :" + format)
}

func readCarJson(filepath string) (carInfos []*rebuilder.CarInfo, err error) {
	b, err := os.ReadFile(filepath)
	if err != nil {
		return
	}
	var list []*rebuilder.CarInfo
	if err = json.Unmarshal(b, &list); err != nil {
		return
	}
	m := make(map[string]*rebuilder.CarInfo)
	for _, cj := range list {
		if !httpDownloadURL(cj.CarFileUrl) {
			return nil, errors.New("invalid download URL")
		}
		if _, ok := m[cj.CarFileUrl]; !ok {
			info := &rebuilder.CarInfo{
				CarFileUrl: cj.CarFileUrl,
			}
			carInfos = append(carInfos, info)
			m[cj.CarFileUrl] = info
		}
		info := m[cj.CarFileUrl]
		info.Deals = append(info.Deals, cj.Deals...)
	}
	return
}

const (
	filedCarFileURL = "car_file_url"
	fieldCarDeals   = "deals"
	filedPayloadCid = "pay_load_cid"
)

func readCarCsv(filepath string) (carInfos []*rebuilder.CarInfo, err error) {
	records, err := ReadCSVFile(filepath)
	if err != nil {
		return
	}
	colMap := make(map[string]int)
	m := make(map[string]*rebuilder.CarInfo)
	for row, fields := range records {
		if row == 0 {
			for i, field := range fields {
				colMap[field] = i
			}
			if _, ok := colMap[filedCarFileURL]; !ok {
				return nil, fmt.Errorf("not found column %s", filedCarFileURL)
			}
			continue
		}
		carURL := fields[colMap[filedCarFileURL]]
		if !httpDownloadURL(carURL) {
			return nil, errors.New("invalid download URL")
		}

		if _, ok := m[carURL]; !ok {
			info := &rebuilder.CarInfo{
				CarFileUrl: carURL,
			}
			carInfos = append(carInfos, info)
			m[carURL] = info
		}
		info := m[carURL]
		if col, ok := colMap[filedPayloadCid]; ok {
			info.CID = fields[col]
		}
		if col, ok := colMap[fieldCarDeals]; ok && fields[col] != "" {
			var deals []*rebuilder.CarDeal
			if err = json.Unmarshal([]byte(fields[col]), &deals); err != nil {
				return
			}
			info.Deals = append(info.Deals, deals...)
		}
	}
	return
}

func ReadCSVFile(filepath string) ([][]string, error) {
	b, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	reader := csv.NewReader(bytes.NewReader(b))
	records, err := reader.ReadAll()
	if err != nil {
		log.Warn(err)
		return readRawCSVFile(b)
	}
	return records, nil
}

func ReadRawCSVFile(filepath string) ([][]string, error) {
	b, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	return readRawCSVFile(b)
}

func readRawCSVFile(b []byte) ([][]string, error) {
	scanner := bufio.NewScanner(bytes.NewReader(b))
	scanner.Split(bufio.ScanLines)
	var records [][]string
	colsCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if len(records) == 0 {
			cols := strings.Split(line, ",")
			records = append(records, cols)
			colsCount = len(cols)
		} else {
			cols := make([]string, colsCount)
			index := 0
			for i := 0; i < colsCount; i++ {
				if i == colsCount-1 {
					cols[i] = line[index:]
					break
				}
				idx := strings.Index(line[index:], ",")
				if idx == -1 {
					break
				}
				cols[i] = line[index : index+idx]
				index += idx + 1
			}
			records = append(records, cols)
		}
	}
	return records, nil
}

func tomlMarshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := toml.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
