package main

import (
	"fmt"
	"os"
	"zedis/config"
	"zedis/logger"
	redisServer "zedis/redis/server"
	"zedis/tcp"
)

var defaultConfig = &config.ServerConfig{
	RunId:      config.GenRandomRunID(40),
	Bind:       "0.0.0.0",
	Port:       6379,
	MaxClients: 100,
}

var banner = `
   ______          ___
      / /___  ____/ (_)____
     / / __ \/ __  / / ___/
   / / /_/ / /_/ / (__  )
 /___/\____/\__,_/_/____/
`

func fileExists(filePath string) bool {
	info, err := os.Stat(filePath)
	return err == nil && !info.IsDir()
}

func main() {
	print(banner)
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "zedis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})

	if fileExists("redis.yaml") {
		config.SetupConfig("redis.yaml")
	} else {
		config.Config = defaultConfig
	}

	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Config.Bind, config.Config.Port),
	}, redisServer.NewHandler())
	if err != nil {
		logger.Error(err)
	}

}
