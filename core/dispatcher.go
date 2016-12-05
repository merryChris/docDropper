package core

import (
	"log"
	"runtime"

	"github.com/huichen/murmur"
	"github.com/huichen/sego"
	"github.com/huichen/wukong/engine"
	pb "github.com/merryChris/docDropper/protos"
	"github.com/spf13/viper"
)

var (
	SegmenterBufferSize = runtime.NumCPU()
)

type Dispatcher struct {
	initialized            bool
	numSego                int
	segmenter              sego.Segmenter
	stopper                engine.StopTokens
	client                 *PlatformClient
	segmenterAddChannel    []chan SegoReq
	segmenterReturnChannel chan pb.CorpusRequest
}

// NewDispatcher 生成 Dispatcher，用来分发训练数据
func NewDispatcher(client *PlatformClient, conf *viper.Viper, numSegmenter int) (*Dispatcher, error) {
	d := &Dispatcher{numSego: numSegmenter, segmenter: sego.Segmenter{}, stopper: engine.StopTokens{}, client: client}

	if err := InitOrm(conf.GetString("username"),
		conf.GetString("password"),
		conf.GetString("host"),
		conf.GetString("port"),
		conf.GetString("dbname"),
		conf.GetInt("max_idle_connections"),
		conf.GetInt("max_open_connections")); err != nil {
		return nil, err
	}

	d.segmenter.LoadDictionary("data/dictionary.txt")
	d.stopper.Init("data/stop_tokens.txt")

	d.segmenterAddChannel = make([]chan SegoReq, numSegmenter)
	d.segmenterReturnChannel = make(chan pb.CorpusRequest, SegmenterBufferSize)
	for shard := 0; shard < d.numSego; shard++ {
		d.segmenterAddChannel[shard] = make(chan SegoReq, SegmenterBufferSize)
	}
	for shard := 0; shard < d.numSego; shard++ {
		go d.segmenterWorker(shard)
	}
	go d.Collect()

	d.initialized = true
	return d, nil
}

func (d *Dispatcher) Dispatch() {
	wangjiaNews := &NewsList{}
	if err := wangjiaNews.GetWangjiaNews(5); err != nil {
		panic(err)
	}

	for _, news := range wangjiaNews.Units {
		shard := murmur.Murmur3([]byte(news.Title)) % uint32(d.numSego)
		d.segmenterAddChannel[shard] <- SegoReq{Title: news.Title, Content: news.Content}
	}
}

func (d *Dispatcher) Collect() {
	for {
		doc := <-d.segmenterReturnChannel
		if err := d.client.StreamingDoc(&doc); err != nil {
			log.Fatal(err)
		}
	}
}

func (d *Dispatcher) Close() {
	if d.initialized {
		for shard := 0; shard < d.numSego; shard++ {
			close(d.segmenterAddChannel[shard])
		}
		close(d.segmenterReturnChannel)
		if err := d.client.CloseStreamingDoc(); err != nil {
			panic(err)
		}
		d.initialized = false
	}
}