package conn_pool

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func Test_borrow_return(t *testing.T) {
	should := require.New(t)
	conn, err := Borrow("127.0.0.1:9092")
	should.Nil(err)
	conn.Release()
	conn2, err := Borrow("127.0.0.1:9092")
	should.Nil(err)
	should.Equal(conn, conn2)
}
