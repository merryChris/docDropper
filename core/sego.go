package core

import (
	"github.com/huichen/wukong/types"
	pb "github.com/merryChris/docDropper/protos"
)

type SegoReq struct {
	Id      uint64
	Title   string
	Content string
}

type DocIndexData struct {
	Id     uint64
	Tokens []types.TokenData
}

func (d *Dispatcher) segmenterModelWorker(shard int) {
	for {
		req, alive := <-d.segmenterModelAddChannel[shard]
		if !alive {
			break
		}

		fr := pb.FitRequest{}
		fr.Title = make([]string, 0)
		fr.Content = make([]string, 0)

		if req.Title != "" {
			segments := d.segmenter.Segment([]byte(req.Title))
			for _, segment := range segments {
				token := segment.Token().Text()
				if !d.stopper.IsStopToken(token) {
					fr.Title = append(fr.Title, token)
				}
			}
		}
		if req.Content != "" {
			segments := d.segmenter.Segment([]byte(req.Content))
			for _, segment := range segments {
				token := segment.Token().Text()
				if !d.stopper.IsStopToken(token) {
					fr.Content = append(fr.Content, token)
				}
			}
		}

		d.segmenterModelReturnChannel <- fr
	}
}

func (d *Dispatcher) segmenterEngineWorker(shard int) {
	for {
		req, alive := <-d.segmenterEngineAddChannel[shard]
		if !alive {
			break
		}

		tokensMap := make(map[string][]int)
		if req.Title != "" {
			segments := d.segmenter.Segment([]byte(req.Title))
			for _, segment := range segments {
				token := segment.Token().Text()
				if !d.stopper.IsStopToken(token) {
					tokensMap[token] = append(tokensMap[token], segment.Start())
				}
			}
		}
		if req.Content != "" {
			segments := d.segmenter.Segment([]byte(req.Content))
			for _, segment := range segments {
				token := segment.Token().Text()
				if !d.stopper.IsStopToken(token) {
					tokensMap[token] = append(tokensMap[token], segment.Start()+len(req.Title))
				}
			}
		}

		tokensData := make([]types.TokenData, len(tokensMap))
		top := 0
		for k, v := range tokensMap {
			tokensData[top].Text = k
			tokensData[top].Locations = v
			top++
		}
		d.segmenterEngineReturnChannel <- DocIndexData{Id: req.Id, Tokens: tokensData}
	}
}
