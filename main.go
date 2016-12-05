package main

import (
	"flag"
	"fmt"

	"github.com/merryChris/docDropper/core"
	"github.com/spf13/viper"
)

var (
	configFile = flag.String("config", "", "配置文件路径")
)

func main() {
	flag.Parse()

	config := viper.New()
	config.SetConfigType("yaml")
	config.SetConfigFile(*configFile)
	if err := config.ReadInConfig(); err != nil {
		panic(fmt.Sprintf("读取配置文件失败：%s。", err.Error()))
	}
	dbConf := config.Sub("database")
	svConf := config.Sub("server")
	bdConf := config.Sub("backend")

	platformClient, err := core.NewPlatformClient(bdConf.GetString("address"))
	if err != nil {
		panic(err)
	}
	defer platformClient.Close()

	dispatcher, err := core.NewDispatcher(platformClient, dbConf, svConf.GetInt("num_segmenter"))
	if err != nil {
		panic(err)
	}
	defer dispatcher.Close()

	dispatcher.Dispatch()
	for {
	}
}
