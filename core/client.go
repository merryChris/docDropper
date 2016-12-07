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
func (pc *PlatformClient) StreamingDoc(doc *pb.FitRequest) error {
	if !pc.initialized {
		return errors.New("PlatformClient 未初始化。")
	}

	//log.Println(doc.Title)
	if err := pc.fitStream.Send(doc); err != nil {
		return err
	}
	return nil
}

// CloseStreamingDoc 关闭 Fit API 数据流
func (pc *PlatformClient) CloseStreamingDoc() error {
	reply, err := pc.fitStream.CloseAndRecv()
	if err != nil {
		return err
	}
	log.Printf("Closed 'Fit' API with %s.", reply.Message)
	return nil
}

// Close 关闭到服务器的连接
func (pc *PlatformClient) Close() {
	if pc.initialized {
		pc.conn.Close()
		pc.initialized = false
	}
}
