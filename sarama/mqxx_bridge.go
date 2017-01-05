package sarama

import (
	"github.com/rcrowley/go-metrics"
)

// TODO: reuse buffer
func Encode(correlationID int32, clientID string, body ProtocolBody) ([]byte, error) {
	req := &request{correlationID: correlationID, clientID: clientID, body: body}
	return encode(req, metrics.DefaultRegistry)
}

func DecodeHeader(buf []byte) (int32, int32, error) {
	header := responseHeader{}
	helper := realDecoder{raw: buf}
	err := header.decode(&helper)
	if err != nil {
		return 0, 0, err
	}

	if helper.off != len(buf) {
		return 0, 0, PacketDecodingError{"invalid length"}
	}

	return header.correlationID, header.length - 4, err
}

func Decode(buf []byte, body VersionedDecoder) error {
	helper := realDecoder{raw: buf}
	err := body.decode(&helper, 0)
	if err != nil {
		return err
	}

	if helper.off != len(buf) {
		return PacketDecodingError{"invalid length"}
	}

	return nil
}