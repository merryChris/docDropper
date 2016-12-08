package core

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"

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
	healthy                bool
	numSego                int
	segmenter              sego.Segmenter
	stopper                engine.StopTokens
	client                 *PlatformClient
	numDocsAdded           uint64
	numDocsSent            uint64
	segmenterAddChannel    []chan SegoReq
	segmenterReturnChannel chan pb.FitRequest

	docsLock struct {
		sync.RWMutex
		mapper map[string]*BriefNews
	}
}

// NewDispatcher 生成 Dispatcher，用来分发训练数据
func NewDispatcher(client *PlatformClient, conf *viper.Viper, numSegmenter int) (*Dispatcher, error) {
	d := &Dispatcher{healthy: true, numSego: numSegmenter, segmenter: sego.Segmenter{},
		stopper: engine.StopTokens{}, client: client}

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
	d.numDocsAdded = uint64(0)
	d.numDocsSent = uint64(0)
	d.docsLock.mapper = make(map[string]*BriefNews)

	d.segmenterAddChannel = make([]chan SegoReq, numSegmenter)
	d.segmenterReturnChannel = make(chan pb.FitRequest, SegmenterBufferSize)
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

func (d *Dispatcher) Dispatch() error {
	if !d.initialized {
		return errors.New("Dispatcher 尚未初始化")
	}

	wangjiaNews := &NewsList{}
	if err := wangjiaNews.GetWangjiaNews(5); err != nil {
		return err
	}

	hf := sha1.New()
	d.docsLock.Lock()
	for _, news := range wangjiaNews.Units {
		hf.Write([]byte(news.Title))
		hash := fmt.Sprintf("%x", hf.Sum(nil))
		d.docsLock.mapper[hash] = &BriefNews{Id: news.Id, Source: news.Source, Title: news.Title}
		shard := murmur.Murmur3([]byte(news.Title)) % uint32(d.numSego)
		d.segmenterAddChannel[shard] <- SegoReq{Hash: hash, Title: news.Title, Content: news.Content}
	}
	d.docsLock.Unlock()

	atomic.AddUint64(&d.numDocsAdded, uint64(len(wangjiaNews.Units)))
	for {
		runtime.Gosched()
		if d.numDocsAdded == d.numDocsSent || !d.healthy {
			break
		}
	}
	return nil
}

func (d *Dispatcher) Search(text string) ([]*BriefNews, error) {
	if !d.initialized {
		return nil, errors.New("Dispatcher 尚未初始化")
	}

	keywords := make([]string, 0)
	segments := d.segmenter.Segment([]byte(text))
	for _, segment := range segments {
		token := segment.Token().Text()
		if !d.stopper.IsStopToken(token) {
			keywords = append(keywords, token)
		}
	}

	query := &pb.QueryRequest{Keywords: keywords}
	outputHashs, err := d.client.FeedingKeywords(query)
	if err != nil {
		return nil, err
	}

	bns := make([]*BriefNews, len(outputHashs))
	d.docsLock.RLock()
	defer d.docsLock.RUnlock()
	for i, hash := range outputHashs {
		if bn, found := d.docsLock.mapper[hash]; found {
			bns[i] = bn
		}
	}
	return bns, nil
}

func (d *Dispatcher) Collect() {
	for {
		doc, alive := <-d.segmenterReturnChannel
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
			close(d.segmenterAddChannel[shard])
		}
		close(d.segmenterReturnChannel)
		if err := d.client.CloseStreamingDoc(); err != nil {
			panic(err)
		}
		d.initialized = false
	}
}
