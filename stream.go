package mqxx

import (
	"sync/atomic"
	"github.com/esdb/mqxx/sarama"
	"github.com/esdb/mqxx/conn_pool"
	"io"
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

func Borrow() *Stream {
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

// GetOffset queries the cluster to get the most recent available offset at the
// given time on the topic/partition combination. Time should be OffsetOldest for
// the earliest available offset, OffsetNewest for the offset of the message that
// will be produced next, or a time.
func (stream *Stream) GetOffset(address string, req *sarama.OffsetRequest) (*sarama.OffsetResponse, error) {
	resp := &sarama.OffsetResponse{}
	return resp, stream.execute(address, req, resp)
}

func (stream *Stream) Produce(address string, req *sarama.ProduceRequest) (*sarama.ProduceResponse, error) {
	resp := &sarama.ProduceResponse{}
	return resp, stream.execute(address, req, resp)
}

func (stream *Stream) Release() {
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

func nextCorrelationId() int32 {
	return atomic.AddInt32(&globalIdGen, 1)
}