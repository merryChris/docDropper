package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/merryChris/docDropper/core"
	"github.com/merryChris/docDropper/web"
	"github.com/spf13/viper"
)

var (
	configFile   = flag.String("config", "", "配置文件路径")
	staticFolder = flag.String("static", "", "静态文件目录")
)

func main() {
	flag.Parse()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for _ = range c {
			log.Println("捕获中断，退出服务器")
			os.Exit(0)
		}
	}()

	config := viper.New()
	config.SetConfigType("yaml")
	config.SetConfigFile(*configFile)
	if err := config.ReadInConfig(); err != nil {
		panic(fmt.Sprintf("读取配置文件失败：%s。", err.Error()))
	}
	dbConf := config.Sub("database")
	srvConf := config.Sub("server")
	bkdConf := config.Sub("backend")

	platformClient, err := core.NewPlatformClient(bkdConf.GetString("address"))
	if err != nil {
		panic(err)
	}
	defer platformClient.Close()

	dispatcher, err := core.NewDispatcher(platformClient, dbConf, srvConf.GetInt("num_segmenter"))
	if err != nil {
		panic(err)
	}
	defer dispatcher.Close()

	greeter, err := web.NewGreeter(dispatcher)
	if err != nil {
		panic(err)
	}
	defer greeter.Close()

	http.HandleFunc("/search", greeter.SearchJsonHandler)
	http.Handle("/", http.FileServer(http.Dir(*staticFolder)))
	log.Println("DocDropper Web 服务启动成功")
	log.Fatal(http.ListenAndServe(srvConf.GetString("address"), nil))
}
