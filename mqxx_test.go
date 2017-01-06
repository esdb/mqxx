package mqxx

import "testing"
import (
	"github.com/stretchr/testify/require"
	"fmt"
	"github.com/esdb/mqxx/sarama"
)

func Test_produce(t *testing.T) {
	should := require.New(t)
	stream := NewStream()
	defer stream.Close()
	resp, err := stream.GetMetadata("10.94.112.88:9092", "test")
	should.Nil(err)
	partition := resp.Topics[0].Partitions[0]
	addr := resp.Brokers[partition.Leader].Addr()
	partitionId := partition.ID
	offsetReq := &sarama.OffsetRequest{}
	offsetReq.AddBlock("test", partitionId, OffsetNewest, 1)
	offsetResp ,err := stream.GetOffset(addr, offsetReq)
	should.Nil(err)
	fmt.Println(offsetResp.GetBlock("test", partitionId).Offsets[0])
	produceReq := &sarama.ProduceRequest{}
	msg := &sarama.Message{Codec: sarama.CompressionNone, Key: nil, Value: []byte("hello")}
	produceReq.AddMessage("test", partitionId, msg)
	produceResp, err := stream.Produce(addr, produceReq, sarama.WaitForLocal)
	should.Nil(err)
	fmt.Println(produceResp.GetBlock("test", partitionId))
}

func Test_produce_async(t *testing.T) {
	should := require.New(t)
	stream := NewStream()
	defer stream.Close()
	resp, err := stream.GetMetadata("10.94.112.88:9092", "test")
	should.Nil(err)
	partition := resp.Topics[0].Partitions[0]
	addr := resp.Brokers[partition.Leader].Addr()
	partitionId := partition.ID
	offsetReq := &sarama.OffsetRequest{}
	offsetReq.AddBlock("test", partitionId, OffsetNewest, 1)
	offsetResp ,err := stream.GetOffset(addr, offsetReq)
	should.Nil(err)
	fmt.Println(offsetResp.GetBlock("test", partitionId).Offsets[0])
	produceReq := &sarama.ProduceRequest{}
	msg := &sarama.Message{Codec: sarama.CompressionNone, Key: nil, Value: []byte("hello")}
	produceReq.AddMessage("test", partitionId, msg)
	produceResp, err := stream.ProduceAsync(addr, produceReq)
	should.Nil(err)
	fmt.Println(produceResp.GetBlock("test", partitionId))
}

func Test_consume(t *testing.T) {
	should := require.New(t)
	stream := NewStream()
	defer stream.Close()
	resp, err := stream.GetMetadata("10.94.112.88:9092", "test")
	should.Nil(err)
	partition := resp.Topics[0].Partitions[0]
	addr := resp.Brokers[partition.Leader].Addr()
	partitionId := partition.ID
	offsetReq := &sarama.OffsetRequest{}
	offsetReq.AddBlock("test", partitionId, OffsetOldest, 1)
	offsetResp ,err := stream.GetOffset(addr, offsetReq)
	should.Nil(err)
	fmt.Println(offsetResp.GetBlock("test", partitionId).Offsets[0])
}