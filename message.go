package mqxx

import (
	"github.com/esdb/mqxx/sarama"
)

type MessageSetBuilder struct {
	partitioner Partitioner
	byTopic     topicToPartition
}

type Partitioner func(topic string, key string, value string) int32
type partitionToMessage map[int32]*sarama.MessageSet
type topicToPartition map[string]partitionToMessage

func NewBuilder(partitioner Partitioner) *MessageSetBuilder {
	return &MessageSetBuilder{partitioner, topicToPartition{}}
}

func (builder *MessageSetBuilder) Add(topic string, key string, value string) {
	partitionId := builder.partitioner(topic, key, value)
	byPartition := builder.byTopic[topic]
	if byPartition == nil {
		byPartition = partitionToMessage{}
		builder.byTopic[topic] = byPartition
	}
	set := byPartition[partitionId]
	if set == nil {
		set = &sarama.MessageSet{}
		byPartition[partitionId] = set
	}
	set.AddMessage(&sarama.Message{
		Key: []byte(key), // TODO: avoid copy
		Value: []byte(value),
	})
}

func (builder *MessageSetBuilder) Count(topic string, partitionId int32) int {
	byPartition := builder.byTopic[topic]
	if byPartition == nil {
		return 0
	}
	set := byPartition[partitionId]
	if set == nil {
		return 0
	}
	return len(set.Messages)
}

func (builder *MessageSetBuilder) Flush(topic string, partitionId int32, codec sarama.CompressionCodec) *sarama.ProduceRequest {
	byPartition := builder.byTopic[topic]
	if byPartition == nil {
		return nil
	}
	set := byPartition[partitionId]
	if set == nil {
		return nil
	}
	delete(byPartition, partitionId)
	req := &sarama.ProduceRequest{}
	if codec == sarama.CompressionNone {
		req.AddSet(topic, partitionId, set)
	} else {
		req.AddMessage(topic, partitionId, &sarama.Message{
			Codec: codec,
			Key: nil,
			Set: set,
		})
	}
	return req
}