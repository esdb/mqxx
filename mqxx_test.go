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
	resp, err := stream.GetMetadata("127.0.0.1:9092", "test")
	should.Nil(err)
	partition := resp.Topics[0].Partitions[0]
	addr := resp.Brokers[partition.Leader].Addr()
	partitionId := partition.ID
	offsetReq := &sarama.OffsetRequest{}
	offsetReq.AddBlock("test", partitionId, OffsetNewest, 1)
	offsetResp ,err := stream.GetOffset(addr, offsetReq)
	should.Nil(err)
	fmt.Println(offsetResp.GetBlock("test", partitionId).Offsets[0])
	builder := NewBuilder(func(topic string, key string, value string) int32 {
		return partitionId
	})
	builder.Add("test", "", "hello")
	builder.Add("test", "", "world")
	produceReq := builder.Flush("test", partitionId, sarama.CompressionSnappy)
	produceResp, err := stream.Produce(addr, produceReq, sarama.WaitForLocal)
	should.Nil(err)
	fmt.Println(produceResp.GetBlock("test", partitionId))
}

func Test_produce_async(t *testing.T) {
	should := require.New(t)
	stream := NewStream()
	defer stream.Close()
	resp, err := stream.GetMetadata("127.0.0.1:9092", "test")
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

func Test_fetch(t *testing.T) {
	should := require.New(t)
	stream := NewStream()
	defer stream.Close()
	resp, err := stream.GetMetadata("127.0.0.1:9092", "test")
	should.Nil(err)
	partition := resp.Topics[0].Partitions[0]
	addr := resp.Brokers[partition.Leader].Addr()
	partitionId := partition.ID
	fmt.Println(partitionId)
	offsetReq := &sarama.OffsetRequest{}
	offsetReq.AddBlock("test", partitionId, OffsetOldest, 1)
	offsetResp ,err := stream.GetOffset(addr, offsetReq)
	should.Nil(err)
	offset := offsetResp.GetBlock("test", partitionId).Offsets[0]
	fetchReq := &sarama.FetchRequest{}
	fetchReq.AddBlock("test", partitionId, offset, 1024 * 32)
	fetchResp, err := stream.Fetch(addr, fetchReq)
	should.Nil(err)
	msgs := fetchResp.GetBlock("test", partitionId).MsgSet.AllMessages()
	fmt.Println(len(msgs))
	for _, msg := range msgs {
		fmt.Println(string(msg.Value))
	}
}

func Test_group(t *testing.T) {
	should := require.New(t)
	stream := NewStream()
	defer stream.Close()
	resp, err := stream.GetMetadata("127.0.0.1:9092", "mqxx")
	should.Nil(err)
	partition := resp.Topics[0].Partitions[0]
	addr := resp.Brokers[partition.Leader].Addr()
	partitionId := partition.ID
	offsetReq := &sarama.OffsetRequest{}
	offsetReq.AddBlock("mqxx", partitionId, OffsetOldest, 1)
	offsetResp ,err := stream.GetOffset(addr, offsetReq)
	should.Nil(err)
	fmt.Println(offsetResp.GetBlock("mqxx", partitionId).Offsets[0])
	consumerMetadataResp, err := stream.GetConsumerMetadata(addr, "mqxx")
	should.Nil(err)
	coordinator := consumerMetadataResp.Coordinator.Addr()
	joinGroupReq := &sarama.JoinGroupRequest{
		GroupId:        "mqxx",
		MemberId:       "mqxx",
		SessionTimeout: 30 * 1000,
		ProtocolType:   "consumer",
	}
	meta := &sarama.ConsumerGroupMemberMetadata{
		Version: 1,
		Topics:  []string{"mqxx"},
	}
	should.Nil(joinGroupReq.AddGroupProtocolMetadata(string(StrategyRange), meta))
	should.Nil(joinGroupReq.AddGroupProtocolMetadata(string(StrategyRoundRobin), meta))
	joinGroupResp, err := stream.JoinGroup(coordinator, joinGroupReq)
	should.Nil(err)
	fmt.Println(joinGroupResp.Err)
	fmt.Println(joinGroupResp.GenerationId)
	fmt.Println(joinGroupResp.LeaderId)
	fmt.Println(joinGroupResp.MemberId)
}