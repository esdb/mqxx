package mqxx

import (
	"sync/atomic"
	"github.com/esdb/mqxx/sarama"
	"github.com/esdb/mqxx/conn_pool"
	"io"
	"errors"
)

const (
	// OffsetNewest stands for the log head offset, i.e. the offset that will be
	// assigned to the next message that will be produced to the partition. You
	// can send this to a client's GetOffset method to get this offset, or when
	// calling ConsumePartition to start consuming new messages.
	OffsetNewest int64 = -1
	// OffsetOldest stands for the oldest offset available on the broker for a
	// partition. You can send this to a client's GetOffset method to get this
	// offset, or when calling ConsumePartition to start consuming from the
	// oldest offset that is still available on the broker.
	OffsetOldest int64 = -2
)

// Strategy for partition to consumer assignement
type Strategy string

const (
	// StrategyRange is the default and assigns partition ranges to consumers.
	// Example with six partitions and two consumers:
	//   C1: [0, 1, 2]
	//   C2: [3, 4, 5]
	StrategyRange Strategy = "range"

	// StrategyRoundRobin assigns partitions by alternating over consumers.
	// Example with six partitions and two consumers:
	//   C1: [0, 2, 4]
	//   C2: [1, 3, 5]
	StrategyRoundRobin Strategy = "roundrobin"
)

var streams chan *Stream
var globalIdGen int32

func init() {
	streams = make(chan *Stream, 1024)
}

type Stream struct {
	correlationId int32
	buffer        []byte // reusable byte buffer
	channel       chan error
}

/*
stream is not bound to broker, it can talk to any server
it just represent a session of interaction
 */
func NewStream() *Stream {
	select {
	case stream := <-streams:
		return stream
	default:
		stream := &Stream{nextCorrelationId(), make([]byte, 4096), make(chan error)}
		return stream
	}
}

func (stream *Stream) GetMetadata(address string, topics ...string) (*sarama.MetadataResponse, error) {
	req := &sarama.MetadataRequest{topics}
	resp := &sarama.MetadataResponse{}
	return resp, stream.execute(address, req, resp)
}

func (stream *Stream) GetConsumerMetadata(address string, group string) (*sarama.ConsumerMetadataResponse, error) {
	req := &sarama.ConsumerMetadataRequest{group}
	resp := &sarama.ConsumerMetadataResponse{}
	return resp, stream.execute(address, req, resp)
}

// GetOffset queries the cluster to get the most recent available offset at the
// given time on the topic/partition combination. Time should be OffsetOldest for
// the earliest available offset, OffsetNewest for the offset of the message that
// will be produced next, or a time.
func (stream *Stream) GetOffset(address string, req *sarama.OffsetRequest) (*sarama.OffsetResponse, error) {
	resp := &sarama.OffsetResponse{}
	return resp, stream.execute(address, req, resp)
}

func (stream *Stream) Produce(address string, req *sarama.ProduceRequest, requiredAcks sarama.RequiredAcks) (*sarama.ProduceResponse, error) {
	if requiredAcks == sarama.NoResponse {
		return nil, errors.New("required acks can not be NoResponse for sync producer")
	}
	req.RequiredAcks = requiredAcks
	resp := &sarama.ProduceResponse{}
	return resp, stream.execute(address, req, resp)
}

func (stream *Stream) ProduceAsync(address string, req *sarama.ProduceRequest) (*sarama.ProduceResponse, error) {
	if req.RequiredAcks != sarama.NoResponse {
		return nil, errors.New("required acks must be NoResponse for async producer")
	}
	resp := &sarama.ProduceResponse{}
	return resp, stream.send(address, req)
}

func (stream *Stream) Fetch(address string, req *sarama.FetchRequest) (*sarama.FetchResponse, error) {
	resp := &sarama.FetchResponse{}
	return resp, stream.execute(address, req, resp)
}

func (stream *Stream) JoinGroup(address string, req *sarama.JoinGroupRequest) (*sarama.JoinGroupResponse, error) {
	resp := &sarama.JoinGroupResponse{}
	return resp, stream.execute(address, req, resp)
}

func (stream *Stream) Close() {
	select {
	case streams <- stream:
	default:
		break // gc will collect this stream
	}
}

func (stream *Stream) execute(address string, req sarama.ProtocolBody, resp sarama.VersionedDecoder) error {
	reqBytes, err := sarama.Encode(stream.correlationId, "mqxx", req)
	if err != nil {
		return err
	}
	conn, err := conn_pool.Borrow(address)
	if err != nil {
		return err
	}
	err = conn.Send(stream.correlationId, func(packetSize int, reader io.Reader) error {
		if packetSize > len(stream.buffer) {
			stream.buffer = make([]byte, packetSize)
		}
		buf := stream.buffer[:packetSize]
		_, err := io.ReadFull(reader, buf)
		if err != nil {
			stream.channel <- err
			return err
		}
		err = sarama.Decode(buf, resp)
		if err != nil {
			stream.channel <- err
			return err
		}
		stream.channel <- nil
		return nil
	}, reqBytes)
	if err != nil {
		return err
	}
	err = <-stream.channel
	return err
}

func (stream *Stream) send(address string, req sarama.ProtocolBody) error {
	reqBytes, err := sarama.Encode(stream.correlationId, "mqxx", req)
	if err != nil {
		return err
	}
	conn, err := conn_pool.Borrow(address)
	if err != nil {
		return err
	}
	// assign new correlation id, in case got response from server
	return conn.Send(nextCorrelationId(), nil, reqBytes)
}

func nextCorrelationId() int32 {
	return atomic.AddInt32(&globalIdGen, 1)
}