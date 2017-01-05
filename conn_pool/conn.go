package conn_pool

import (
	"net"
	"time"
	"io"
	"github.com/esdb/mqxx/sarama"
	"sync"
)

/*
connection pool
 */

var DefaultDialer *net.Dialer
var connPools *connPoolMap

func init() {
	DefaultDialer = &net.Dialer{
		Timeout:   time.Second,
		KeepAlive: time.Minute,
	}
	connPools = newConnPoolMap()
}

type Conn struct {
	address      string
	obj          net.Conn
	closed       chan bool
	callbacks    map[int32]SendCallback // protected by lock
	callbackLock *sync.Mutex
}

type SendCallback func(packetSize int, conn io.Reader) error

func Borrow(address string) (*Conn, error) {
	pool, found := connPools.read()[address]
	if !found {
		pool = make(chan *Conn, 16)
		connPools.put(address, pool)
	}
	var conn *Conn
	for {
		select {
		case conn = <-pool:
			if conn.isClosed() {
				continue
			}
			return conn, nil
		default:
			netConn, err := DefaultDialer.Dial("tcp", address)
			if err != nil {
				return nil, err
			}
			conn = &Conn{address, netConn, make(chan bool), map[int32]SendCallback{}, &sync.Mutex{}}
			go conn.backgroundPoll()
			return conn, err
		}
	}
}

func (conn *Conn) backgroundPoll() {
	// TODO: add panic recover, add logging
	headerBytes := make([]byte, 8)
	for {
		_, err := io.ReadFull(conn.obj, headerBytes)
		if err != nil {
			// TODO: add logging
			conn.obj.Close()
			close(conn.closed)
			return
		}
		correlationId, packetSize, err := sarama.DecodeHeader(headerBytes)
		callback := conn.removeCallback(correlationId)
		if callback == nil {
			// TODO: add logging
			conn.obj.Close()
			close(conn.closed)
			return
		}
		err = callback(int(packetSize), conn.obj)
		if err != nil {
			// TODO: add logging
			conn.obj.Close()
			close(conn.closed)
			return
		}
	}
}

func (conn *Conn) Release() error {
	pool, found := connPools.read()[conn.address]
	if !found {
		return conn.obj.Close()
	}
	select {
	case pool <- conn:
	default:
		return conn.obj.Close()
	}
	return nil
}

func (conn *Conn) Send(correlationId int32, callback SendCallback, reqBytes []byte) error {
	conn.addCallback(correlationId, callback)
	_, err := conn.obj.Write(reqBytes)
	if err != nil {
		return err
	}
	return nil
}

func (conn *Conn) addCallback(correlationId int32, callback SendCallback) {
	conn.callbackLock.Lock()
	defer conn.callbackLock.Unlock()
	conn.callbacks[correlationId] = callback
}

func (conn *Conn) removeCallback(correlationId int32) SendCallback {
	conn.callbackLock.Lock()
	defer conn.callbackLock.Unlock()
	callback := conn.callbacks[correlationId]
	delete(conn.callbacks, correlationId)
	return callback
}

func (conn *Conn) isClosed() bool {
	select {
	case _ = <-conn.closed:
		return true
	default:
		return false
	}
}