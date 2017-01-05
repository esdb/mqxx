package mqxx

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func Test_borrow(t *testing.T) {
	should := require.New(t)
	stream := Borrow()
	should.NotNil(stream)
	stream.Release()
}
