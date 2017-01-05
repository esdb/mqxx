package conn_pool

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func Test_cow_map(t *testing.T) {
	should := require.New(t)
	m := newConnPoolMap()
	should.Equal(0, len(m.read()))
	m.put("abc", nil)
	should.Equal(1, len(m.read()))
}
