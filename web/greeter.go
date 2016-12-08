package web

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/merryChris/docDropper/core"
)

type Greeter struct {
	initialized bool
	dispatcher  *core.Dispatcher
}

type SearchJsonResponse struct {
	Units []*core.BriefNews `json:"units"`
}

func NewGreeter(d *core.Dispatcher) (*Greeter, error) {
	g := &Greeter{dispatcher: d}
	if err := g.dispatcher.Dispatch(); err != nil {
		return nil, err
	}

	g.initialized = true
	return g, nil
}

func (g *Greeter) SearchJsonHandler(w http.ResponseWriter, r *http.Request) {
	if !g.initialized {
		log.Fatal("Greeter 尚未初始化")
		return
	}

	query := r.URL.Query().Get("query")
	outputs, err := g.dispatcher.Search(query)
	if err != nil {
		log.Fatal(err)
		return
	}

	fmt.Printf("Searching Results for `%s`:\n", query)
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