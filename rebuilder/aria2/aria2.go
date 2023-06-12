package aria2

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	aria2AddURI = "aria2.addUri"
	aria2Status = "aria2.tellStatus"

	contentTypeJson = "application/json; charset=UTF-8"
	contentTypeForm = "application/x-www-form-urlencoded"
)

const (
	StatusError    string = "error"
	StatusWaiting  string = "waiting"
	StatusActive   string = "active"
	StatusComplete string = "complete"
)

type Client struct {
	token     string
	serverUrl string
}

type StatusResp struct {
	Id      string        `json:"id"`
	JsonRpc string        `json:"jsonrpc"`
	Error   *Error        `json:"error"`
	Result  *StatusResult `json:"result"`
}

type Payload struct {
	JsonRpc string        `json:"jsonrpc"`
	Id      string        `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type DownloadOption struct {
	Out string `json:"out"`
	Dir string `json:"dir"`
}

type DownloadResp struct {
	Id      string `json:"id"`
	JsonRpc string `json:"jsonrpc"`
	Error   *Error `json:"error"`
	Gid     string `json:"result"`
}

type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type StatusResult struct {
	BitField        string                  `json:"bitfield"`
	CompletedLength string                  `json:"completedLength"`
	Connections     string                  `json:"connections"`
	Dir             string                  `json:"dir"`
	DownloadSpeed   string                  `json:"downloadSpeed"`
	ErrorCode       string                  `json:"errorCode"`
	ErrorMessage    string                  `json:"errorMessage"`
	Gid             string                  `json:"gid"`
	NumPieces       string                  `json:"numPieces"`
	PieceLength     string                  `json:"pieceLength"`
	Status          string                  `json:"status"`
	TotalLength     string                  `json:"totalLength"`
	UploadLength    string                  `json:"uploadLength"`
	UploadSpeed     string                  `json:"uploadSpeed"`
	Files           []Aria2StatusResultFile `json:"files"`
}

type Aria2StatusResultFile struct {
	CompletedLength string                `json:"completedLength"`
	Index           string                `json:"index"`
	Length          string                `json:"length"`
	Path            string                `json:"path"`
	Selected        string                `json:"selected"`
	Uris            []StatusResultFileUri `json:"uris"`
}

type StatusResultFileUri struct {
	Status string `json:"status"`
	Uri    string `json:"uri"`
}

func NewClient(host string, port int, secret string) *Client {
	return &Client{
		token:     secret,
		serverUrl: fmt.Sprintf("http://%s:%d/jsonrpc", host, port),
	}
}

func (aria2Client *Client) DownloadStatus(gid string) (ok bool, err error) {
	payload := aria2Client.StatusPayload(gid)
	response, err := httpRequest(http.MethodPost, aria2Client.serverUrl, "", payload, nil)
	if err != nil {
		return
	}
	var aria2Status StatusResp
	if err = json.Unmarshal(response, &aria2Status); err != nil {
		return
	}
	if aria2Status.Error != nil {
		return false, errors.New(aria2Status.Error.Message)
	}
	if aria2Status.Result == nil || len(aria2Status.Result.Files) != 1 {
		return false, errors.New("invalid response")
	}

	result := aria2Status.Result
	file := result.Files[0]
	filePath := file.Path
	fileSize, _ := strconv.Atoi(file.Length)

	switch result.Status {
	case StatusError:
		return false, errors.New(result.ErrorMessage)
	case StatusWaiting:
		return
	case StatusActive:
		time.Sleep(time.Second * 5)
		fileInfo, err := os.Stat(filePath)
		var fileSizeDownloaded int64
		if err != nil {
			fileSizeDownloaded = -1
		} else {
			fileSizeDownloaded = fileInfo.Size()
		}
		completedLen, _ := strconv.Atoi(file.CompletedLength)
		var completePercent float64 = 0
		if fileSize > 0 {
			completePercent = float64(completedLen) / float64(fileSize) * 100
		}
		speed, _ := strconv.Atoi(result.DownloadSpeed)
		downloadSpeed := speed / 1024
		fileSizeDownloaded = fileSizeDownloaded / 1024
		fmt.Printf("the current ipfs data file downloading, complete: %.2f%%, speed: %dKiB, downloaded: %dKiB, %s, download gid: %s\n", completePercent, downloadSpeed, fileSizeDownloaded, result.Status, gid)
		return false, nil
	case StatusComplete:
		_, err := os.Stat(filePath)
		if err != nil {
			return false, fmt.Errorf("download gid: %s, error: %s, please check aria2 services", gid, err.Error())
		}
		return true, nil
	}
	return false, fmt.Errorf("invalid download status: %s", result.Status)
}

func (aria2Client *Client) DownloadFile(uri string, outDir, outFilename string) (gid string, err error) {
	payload := aria2Client.DownloadPayload(aria2AddURI, uri, outDir, outFilename)
	response, err := httpRequest(http.MethodPost, aria2Client.serverUrl, "", payload, nil)
	if err != nil {
		return
	}

	var result DownloadResp
	if err = json.Unmarshal(response, &result); err != nil {
		return
	}
	if result.Error != nil {
		return "", errors.New(result.Error.Message)
	}
	return result.Gid, nil
}

func (aria2Client *Client) StatusPayload(gid string) *Payload {
	return &Payload{
		JsonRpc: "2.0",
		Method:  aria2Status,
		Params:  []interface{}{"token:" + aria2Client.token, gid},
	}
}

func (aria2Client *Client) DownloadPayload(method string, uri string, outDir, outFilename string) *Payload {
	options := DownloadOption{
		Out: outFilename,
		Dir: outDir,
	}
	return &Payload{
		JsonRpc: "2.0",
		Id:      uri,
		Method:  method,
		Params:  []interface{}{"token:" + aria2Client.token, []string{uri}, options},
	}
}

func httpRequest(httpMethod, uri, tokenString string, params interface{}, timeout *time.Duration) (body []byte, err error) {
	var req *http.Request
	switch params := params.(type) {
	case io.Reader:
		req, err = http.NewRequest(httpMethod, uri, params)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", contentTypeForm)
	default:
		jsonReq, errJson := json.Marshal(params)
		if errJson != nil {
			return nil, errJson
		}

		req, err = http.NewRequest(httpMethod, uri, bytes.NewBuffer(jsonReq))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", contentTypeJson)
	}

	if len(strings.Trim(tokenString, " ")) > 0 {
		req.Header.Set("Authorization", "Bearer "+tokenString)
	}

	customTransport := http.DefaultTransport.(*http.Transport).Clone()
	customTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}

	client := &http.Client{Transport: customTransport}
	if timeout != nil {
		client.Timeout = *timeout
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status: %s, code:%d, url:%s", resp.Status, resp.StatusCode, uri)
	}
	return io.ReadAll(resp.Body)
}
