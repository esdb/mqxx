package stream_pool

import (
	"sync/atomic"
	"github.com/esdb/mqxx/sarama"
	"github.com/esdb/mqxx/conn_pool"
	"io"
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

func (stream *Stream) Release() {
	select {
	case streams <- stream:
	default:
		break // gc will collect this stream
	}
}

func (stream *Stream) Execute(address string, req sarama.ProtocolBody, resp sarama.VersionedDecoder) error {
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