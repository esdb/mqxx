package mqxx

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func Test_borrow(t *testing.T) {
	should := require.New(t)
	stream := NewStream()
	should.NotNil(stream)
	stream.Close()
}
