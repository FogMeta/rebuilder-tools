package config

import (
	"github.com/BurntSushi/toml"
)

type Config struct {
	DataBase *Database `toml:"db,omitempty"`
	Aria2    *Aria2    `toml:"aria2"`
	Task     *Task     `toml:"task"`
	MCS      *MCS      `toml:"mcs"`
	Lotus    *Lotus    `toml:"lotus"`
	Log      *Log      `toml:"log,omitempty"`
}

type Database struct {
	Host     string `toml:"host"`
	Port     int    `toml:"port"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Database string `toml:"database"`
	Debug    bool   `toml:"debug"`
}

type Aria2 struct {
	Host   string `toml:"host"`
	Port   int    `toml:"port"`
	Secret string `toml:"secret"`
}

type Task struct {
	InputPath  string `toml:"input_path"`
	OutputPath string `toml:"output_path"`
	Parallel   int    `toml:"parallel"`
}

type MCS struct {
	APIKey     string `toml:"api_key"`
	APIToken   string `toml:"api_token"`
	Network    string `toml:"network"`
	BucketName string `toml:"bucket_name"`
}

type Lotus struct {
	NodeApi string `toml:"node_api"`
	Wallet  string `toml:"wallet"`
	Timeout int    `toml:"timeout"`
}

type Log struct {
	Env   string `toml:"env"`
	Level int    `toml:"level"`
}

const (
	defaultConfPath = "config/config.toml"
)

func Init(filepath ...string) (*Config, error) {
	path := defaultConfPath
	if len(filepath) > 0 {
		path = filepath[0]
	}
	var conf Config
	_, err := toml.DecodeFile(path, &conf)
	if err != nil {
		return nil, err
	}
	return &conf, nil
}
