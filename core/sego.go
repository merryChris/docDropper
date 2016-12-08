package core

import pb "github.com/merryChris/docDropper/protos"

type SegoReq struct {
	Hash    string
	Title   string
	Content string
}

func (d *Dispatcher) segmenterWorker(shard int) {
	for {
		req, alive := <-d.segmenterAddChannel[shard]
		if !alive {
			break
		}

		fr := pb.FitRequest{}
		fr.Hash = req.Hash
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

		d.segmenterReturnChannel <- fr
	}
}
