package config

import (
	"gopkg.in/yaml.v3"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type ServerConfig struct {
	RunId        string `yaml:"RunID"` // 运行ID，每次启动都不一样
	Bind         string `yaml:"Bind"`  // 绑定
	Port         int    `yaml:"Port"`  // 服务端口
	Dir          string `yaml:"Dir"`   // 服务运行目录
	AnnounceHost string `yaml:"AnnounceHost"`
	MaxClients   int    `yaml:"MaxClients"`  // 最大客户端数量
	RequirePass  string `yaml:"RequirePass"` // 密码
	Databases    int    `yaml:"Databases"`   // 数据库数量
	ReplTimeout  int    `yaml:"ReplTimeout"` // 服务端响应超时

	ConfigFilePath string `yaml:"configFilePath omitempty"` // 配置文件路径
}

type ServerInfo struct {
	StartUpTime time.Time // 服务启动时间
}

func (p *ServerConfig) AnnounceAddress() string {
	return p.AnnounceHost + ":" + strconv.Itoa(p.Port)
}

var Config *ServerConfig
var EachTimeServerInfo *ServerInfo

func init() {
	EachTimeServerInfo = &ServerInfo{
		StartUpTime: time.Now(),
	}

	Config = &ServerConfig{
		RunId: GenRandomRunID(40),
		Bind:  "127.0.0.1",
		Port:  6379,
	}

}

var numberAndLetters = []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func GenRandomRunID(length int) string {
	res := []byte("ID_")
	for i := 0; i < length; i++ {
		res = append(res, numberAndLetters[rand.Intn(len(numberAndLetters))])
	}
	return string(res)
}

func parseConfigFile(confFilePath string) (*ServerConfig, error) {
	reader, err := os.Open(confFilePath)
	if err != nil {
		panic(err)
	}
	defer reader.Close()
	config := &ServerConfig{}
	fileBytes, err := io.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(fileBytes, config)
	if err != nil {
		return config, err
	}

	if err != nil {
		panic(err)
	}
	return config, nil
}

func SetupConfig(configFilePath string) {
	var err error
	Config, err = parseConfigFile(configFilePath)
	if err != nil {
		panic(err)
	}
	Config.RunId = GenRandomRunID(40)
	absPath, err := filepath.Abs(configFilePath)
	Config.ConfigFilePath = absPath
	if Config.Dir == "" {
		Config.Dir = "."
	}
}

func GetTempDir() string {
	return filepath.Join(Config.Dir, "tmp")
}
