package web

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/merryChris/docDropper/core"
	"github.com/spf13/viper"
)

type Greeter struct {
	initialized bool
	dispatcher  *core.Dispatcher
	topK        int
}

type SearchJsonResponse struct {
	Units []*core.BriefNews `json:"units"`
}

func NewGreeter(d *core.Dispatcher, conf *viper.Viper) (*Greeter, error) {
	g := &Greeter{dispatcher: d}
	if err := g.dispatcher.Dispatch(conf.GetBool("init_model"), uint64(conf.GetInt("num_news"))); err != nil {
		return nil, err
	}
	if err := g.dispatcher.Index(uint64(conf.GetInt("num_news"))); err != nil {
		return nil, err
	}

	g.topK = conf.GetInt("topk")
	g.initialized = true
	return g, nil
}

func (g *Greeter) SearchJsonHandler(w http.ResponseWriter, r *http.Request) {
	if !g.initialized {
		log.Fatal("Greeter 尚未初始化")
		return
	}

	query := r.URL.Query().Get("query")
	outputs, err := g.dispatcher.Search(query, g.topK)
	if err != nil {
		log.Fatal(err)
		return
	}

	fmt.Printf("Here are %d searching results for `%s`:\n", len(outputs), query)
	for i, bn := range outputs {
		if bn != nil {
			fmt.Printf("%d: %d %s %s\n", i+1, bn.Id, bn.Source, bn.Title)
		}
	}

	response, _ := json.Marshal(&SearchJsonResponse{Units: outputs})
	w.Header().Set("Content-Type", "application/json")
	io.WriteString(w, string(response))
}

func (g *Greeter) Close() {
	if g.initialized {
		g.initialized = false
	}
}
