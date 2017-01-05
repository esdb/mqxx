package conn_pool

import (
	"unsafe"
	"sync/atomic"
)

type connPoolMap struct {
	ptr unsafe.Pointer
}

func newConnPoolMap() *connPoolMap {
	return &connPoolMap{
		unsafe.Pointer(&map[string]chan *Conn{}),
	}
}

func (m *connPoolMap) read() map[string]chan *Conn {
	ptr := atomic.LoadPointer(&m.ptr)
	return *(*map[string]chan *Conn)(ptr)
}

func (m *connPoolMap) put(address string, pool chan *Conn) {
	for {
		ptr := atomic.LoadPointer(&m.ptr)
		copy := map[string]chan *Conn{}
		for k, v := range *(*map[string]chan *Conn)(ptr) {
			copy[k] = v
		}
		copy[address] = pool
		if atomic.CompareAndSwapPointer(&m.ptr, ptr, unsafe.Pointer(&copy)) {
			break
		}
	}
}
