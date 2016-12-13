package core

import (
	"errors"
	"log"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/huichen/murmur"
	"github.com/huichen/sego"
	"github.com/huichen/wukong/engine"
	"github.com/huichen/wukong/types"
	pb "github.com/merryChris/docDropper/protos"
	"github.com/spf13/viper"
)

var (
	SegmenterBufferSize = runtime.NumCPU()
)

type Dispatcher struct {
	initialized                  bool
	healthy                      bool
	useModel                     bool
	numSego                      int
	segmenter                    sego.Segmenter
	stopper                      engine.StopTokens
	searcher                     engine.Engine
	client                       *PlatformClient
	numDocsAdded                 uint64
	numDocsSent                  uint64
	numDocsIndexed               uint64
	segmenterModelAddChannel     []chan SegoReq
	segmenterEngineAddChannel    []chan SegoReq
	segmenterModelReturnChannel  chan pb.FitRequest
	segmenterEngineReturnChannel chan DocIndexData

	docsLock struct {
		sync.RWMutex
		mapper map[uint64]*BriefNews
	}
}

// NewDispatcher 生成 Dispatcher，用来分发训练数据，搜索查询
func NewDispatcher(client *PlatformClient, dbConf *viper.Viper, srvConf *viper.Viper) (*Dispatcher, error) {
	d := &Dispatcher{healthy: true, numSego: srvConf.GetInt("num_segmenter"), useModel: srvConf.GetBool("use_model"),
		segmenter: sego.Segmenter{}, stopper: engine.StopTokens{}, client: client}

	if err := InitOrm(dbConf.GetString("username"),
		dbConf.GetString("password"),
		dbConf.GetString("host"),
		dbConf.GetString("port"),
		dbConf.GetString("dbname"),
		dbConf.GetInt("max_idle_connections"),
		dbConf.GetInt("max_open_connections")); err != nil {
		return nil, err
	}

	d.segmenter.LoadDictionary("data/dictionary.txt")
	d.stopper.Init("data/stop_tokens.txt")
	d.numDocsAdded = uint64(0)
	d.numDocsSent = uint64(0)
	d.numDocsIndexed = uint64(0)
	d.docsLock.mapper = make(map[uint64]*BriefNews)

	d.segmenterModelAddChannel = make([]chan SegoReq, d.numSego)
	d.segmenterEngineAddChannel = make([]chan SegoReq, d.numSego)
	d.segmenterModelReturnChannel = make(chan pb.FitRequest, SegmenterBufferSize)
	d.segmenterEngineReturnChannel = make(chan DocIndexData, SegmenterBufferSize)
	for shard := 0; shard < d.numSego; shard++ {
		d.segmenterModelAddChannel[shard] = make(chan SegoReq, SegmenterBufferSize)
		d.segmenterEngineAddChannel[shard] = make(chan SegoReq, SegmenterBufferSize)
	}
	for shard := 0; shard < d.numSego; shard++ {
		go d.segmenterModelWorker(shard)
		go d.segmenterEngineWorker(shard)
	}
	go d.CollectForModel()
	go d.CollectForEngine()

	engineOptions := types.EngineInitOptions{NotUsingSegmenter: true, UsePersistentStorage: false}
	engineOptions.Init()
	d.searcher.Init(engineOptions)

	d.initialized = true
	return d, nil
}

func (d *Dispatcher) Dispatch(initModel bool, numNews uint64) error {
	if !d.initialized {
		return errors.New("Dispatcher 尚未初始化")
	}

	newsList := &NewsList{}
	if err := newsList.GetNews(numNews); err != nil {
		return err
	}

	d.docsLock.Lock()
	for _, news := range newsList.Units {
		d.docsLock.mapper[news.Id] = &BriefNews{Id: news.Id, Source: news.Source, Title: news.Title}
		if initModel {
			shard := murmur.Murmur3([]byte(news.Title)) % uint32(d.numSego)
			d.segmenterModelAddChannel[shard] <- SegoReq{Id: news.Id, Title: news.Title, Content: news.Content}
		}
	}
	d.docsLock.Unlock()

	if initModel {
		atomic.AddUint64(&d.numDocsAdded, uint64(len(newsList.Units)))
		for {
			runtime.Gosched()
			if d.numDocsAdded == d.numDocsSent || !d.healthy {
				break
			}
		}
	}

	return nil
}

func (d *Dispatcher) CollectForModel() {
	for {
		doc, alive := <-d.segmenterModelReturnChannel
		if !alive {
			break
		}

		if err := d.client.StreamingDoc(&doc); err != nil {
			d.healthy = false
			log.Fatal(err)
		}
		atomic.AddUint64(&d.numDocsSent, 1)
	}
}

func (d *Dispatcher) Close() {
	if d.initialized {
		for shard := 0; shard < d.numSego; shard++ {
			close(d.segmenterModelAddChannel[shard])
			close(d.segmenterEngineAddChannel[shard])
		}
		close(d.segmenterModelReturnChannel)
		close(d.segmenterEngineReturnChannel)
		d.initialized = false
	}
}
