package core

import (
	"errors"
	"log"

	pb "github.com/merryChris/docDropper/protos"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type PlatformClient struct {
	initialized bool
	client      pb.PlatformClient
	fitStream   pb.Platform_FitClient
	conn        *grpc.ClientConn
}

// NewPlatformClient 生成 Platform 客户端并和服务器建立连接
// 参数 address 的格式是 ip:port
func NewPlatformClient(address string) (*PlatformClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	pc := &PlatformClient{}
	pc.conn = conn
	pc.client = pb.NewPlatformClient(conn)
	if pc.fitStream, err = pc.client.Fit(context.Background()); err != nil {
		return nil, err
	}
	pc.initialized = true
	return pc, nil
}

// StreamingDoc 将数据传送给 Platform 训练数据
func (pc *PlatformClient) StreamingDoc(fr *pb.FitRequest) error {
	if !pc.initialized {
		return errors.New("PlatformClient 未初始化。")
	}

	if err := pc.fitStream.Send(fr); err != nil {
		return err
	}
	return nil
}

// FilteringTokens 将 tokens 传送给 Platform 进行过滤
func (pc *PlatformClient) FilteringTokens(fr *pb.FilterRequest) (bool, []string, error) {
	if !pc.initialized {
		return false, nil, errors.New("PlatformClient 未初始化。")
	}

	resp, err := pc.client.Filter(context.Background(), fr)
	if err != nil {
		return false, nil, err
	}
	return resp.Ready, resp.Tokens, nil

}

// FeedingKeywords 将查询传送给 Platform 进行查询
func (pc *PlatformClient) FeedingKeywords(qr *pb.QueryRequest) ([]string, []float32, error) {
	if !pc.initialized {
		return nil, nil, errors.New("PlatformClient 未初始化。")
	}

	resp, err := pc.client.Query(context.Background(), qr)
	if err != nil {
		return nil, nil, err
	}
	return resp.Keywords, resp.Probabilities, nil
}

// CloseStreamingDoc 关闭 Fit API 数据流
func (pc *PlatformClient) CloseStreamingDoc() error {
	reply, err := pc.fitStream.CloseAndRecv()
	if err != nil {
		return err
	}
	log.Printf("Closed `Fit` API with `%s`.\n", reply.Message)
	return nil
}

// Close 关闭到服务器的连接
func (pc *PlatformClient) Close() {
	if pc.initialized {
		if err := pc.conn.Close(); err != nil {
			panic(err)
		}
		pc.initialized = false
	}
}
