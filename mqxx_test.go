package mqxx

import "testing"
import (
	"github.com/esdb/mqxx/sarama"
	"github.com/stretchr/testify/require"
	"fmt"
)

func Test_get_topic(t *testing.T) {
	should := require.New(t)
	resp, err := GetMetadata("127.0.0.1:9092", &sarama.MetadataRequest{Topics: []string{"test"}})
	should.Nil(err)
	fmt.Println(resp.Topics[0].Partitions[0].Replicas)
}