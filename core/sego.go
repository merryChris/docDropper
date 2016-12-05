package core

import (
	pb "github.com/merryChris/docDropper/protos"
)

type SegoReq struct {
	Title   string
	Content string
}

func (d *Dispatcher) segmenterWorker(shard int) {
	for {
		req := <-d.segmenterAddChannel[shard]
		cr := pb.CorpusRequest{}
		cr.Title = make([]string, 0)
		cr.Content = make([]string, 0)

		if req.Title != "" {
			segments := d.segmenter.Segment([]byte(req.Title))
			for _, segment := range segments {
				token := segment.Token().Text()
				if !d.stopper.IsStopToken(token) {
					cr.Title = append(cr.Title, token)
				}
			}
		}
		if req.Content != "" {
			segments := d.segmenter.Segment([]byte(req.Content))
			for _, segment := range segments {
				token := segment.Token().Text()
				if !d.stopper.IsStopToken(token) {
					cr.Content = append(cr.Content, token)
				}
			}
		}

		d.segmenterReturnChannel <- cr
	}
}
