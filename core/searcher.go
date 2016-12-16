package core

import (
	"errors"
	"log"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/huichen/murmur"
	"github.com/huichen/wukong/types"
	pb "github.com/merryChris/docDropper/protos"
)

func (d *Dispatcher) Index(numNews uint64) error {
	if !d.initialized {
		return errors.New("Dispatcher 尚未初始化")
	}

	newsList := &NewsList{}
	if err := newsList.GetNews(numNews); err != nil {
		return err
	}

	for _, news := range newsList.Units {
		shard := murmur.Murmur3([]byte(news.Title)) % uint32(d.numSego)
		d.segmenterEngineAddChannel[shard] <- SegoReq{Id: news.Id, Title: news.Title, Content: news.Content}
	}

	atomic.AddUint64(&d.numDocsAdded, uint64(len(newsList.Units)))
	for {
		d.searcher.FlushIndex()
		runtime.Gosched()
		if d.numDocsAdded == d.numDocsIndexed || !d.healthy {
			break
		}
	}

	if d.statsMode {
		log.Printf("Indexing Statistics Info: %d %d %d %d %d\n", d.totalLength, d.totalBytes, d.filteredLength, d.filteredBytes, len(d.tokenDic))
	}

	return nil
}

func (d *Dispatcher) CollectForEngine() {
	for {
		doc, alive := <-d.segmenterEngineReturnChannel
		if !alive {
			break
		}

		if d.statsMode {
			for _, token := range doc.Tokens {
				d.totalLength += len([]rune(token.Text)) * len(token.Locations)
				d.totalBytes += len(token.Text) * len(token.Locations)
				d.tokenDic[token.Text] = true
			}
		}

		if d.useModel {
			tokens := make([]string, len(doc.Tokens))
			for i, t := range doc.Tokens {
				tokens[i] = t.Text
			}

			for {
				ready, filteredTokens, err := d.client.FilteringTokens(&pb.FilterRequest{Tokens: tokens})
				if err != nil {
					d.healthy = false
					log.Fatal(err)
				}
				if !ready {
					wait := time.After(time.Second * 10)
					<-wait
					continue
				}

				j := 0
				for i, ft := range filteredTokens {
					for doc.Tokens[j].Text != ft {
						j++
					}
					if i < j {
						doc.Tokens[i] = doc.Tokens[j]
						j++
					}
				}
				doc.Tokens = doc.Tokens[:len(filteredTokens)]
				break
			}
		}
		if d.statsMode {
			for _, token := range doc.Tokens {
				d.filteredLength += len([]rune(token.Text)) * len(token.Locations)
				d.filteredBytes += len(token.Text) * len(token.Locations)
			}
		}
		d.searcher.IndexDocument(doc.Id, types.DocumentIndexData{Tokens: doc.Tokens}, false)
		atomic.AddUint64(&d.numDocsIndexed, 1)
	}
}

func (d *Dispatcher) Search(text string, topK int) ([]*BriefNews, error) {
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

	docIds := make([]uint64, 0)
	found := false
	if d.useModel {
		query := &pb.QueryRequest{Keywords: keywords}
		ready, context, probabilities, err := d.client.FeedingKeywords(query)
		if err != nil {
			return nil, err
		}

		if ready {
			tokenScores := make(map[uint64]float32)
			for i, token := range context {
				docs := d.searcher.Search(types.SearchRequest{Tokens: []string{token}})
				for _, doc := range docs.Docs {
					//NOTE: (zacky, 2016.DEC.12th) JUST IGNORE `doc.Scores` HERE.
					tokenScores[doc.DocId] += probabilities[i]
				}
			}
			// 插入排序
			for d, _ := range tokenScores {
				if len(docIds) <= topK {
					docIds = append(docIds, d)
				} else {
					docIds[topK] = d
				}
				for i := len(docIds) - 1; i > 0; i-- {
					if tokenScores[docIds[i]] < tokenScores[docIds[i-1]] {
						break
					}
					docIds[i], docIds[i-1] = docIds[i-1], docIds[i]
				}
			}
			found = true
		}
	}

	if !d.useModel || !found {
		docs := d.searcher.Search(types.SearchRequest{Tokens: keywords})
		for i := 0; i < len(docs.Docs) && i < topK; i++ {
			docIds = append(docIds, docs.Docs[i].DocId)
		}
	}
	if len(docIds) > topK {
		docIds = docIds[:topK]
	}

	bns := make([]*BriefNews, len(docIds))
	d.docsLock.RLock()
	defer d.docsLock.RUnlock()
	for i, docId := range docIds {
		if bn, found := d.docsLock.mapper[docId]; found {
			bns[i] = bn
		}
	}

	return bns, nil
}
