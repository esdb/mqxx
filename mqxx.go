package mqxx

import (
	"github.com/esdb/mqxx/sarama"
	"github.com/esdb/mqxx/stream_pool"
)

func GetMetadata(address string, req *sarama.MetadataRequest) (*sarama.MetadataResponse, error) {
	stream := stream_pool.Borrow()
	defer stream.Release()
	resp := &sarama.MetadataResponse{}
	err := stream.Execute(address, req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}