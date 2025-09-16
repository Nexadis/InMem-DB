package tcp

import (
	"net"
	"time"
)

type idleRW struct {
	net.Conn
	idle time.Duration
}

func (i *idleRW) Read(p []byte) (n int, err error) {
	i.SetDeadline(time.Now().Add(i.idle))
	return i.Conn.Read(p)
}

func (i *idleRW) Write(p []byte) (n int, err error) {
	i.SetDeadline(time.Now().Add(i.idle))
	return i.Conn.Write(p)
}

func withIdle(conn net.Conn, idle time.Duration) net.Conn {
	return &idleRW{
		Conn: conn,
		idle: idle,
	}
}
