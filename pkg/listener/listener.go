package listener

import (
	"context"
	"net"
	"sync"
)

type listener struct {
	net.Listener

	slots chan struct{}
	done  chan struct{}
	once  sync.Once
}

type connection struct {
	net.Conn

	release func()
	once    sync.Once
}

func Listen(ctx context.Context, address string, maximum int) (net.Listener, error) {
	var cfg net.ListenConfig

	l, err := cfg.Listen(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}

	return &listener{
		Listener: l,
		slots:    make(chan struct{}, maximum),
		done:     make(chan struct{}),
	}, nil
}

func (l *listener) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	case <-l.done:
		return nil, net.ErrClosed
	}

	c, err := l.Listener.Accept()
	if err != nil {
		<-l.slots

		return nil, err
	}

	return &connection{
		Conn: c,
		release: func() {
			<-l.slots
		},
	}, nil
}

func (l *listener) Close() error {
	l.once.Do(func() {
		close(l.done)
	})

	return l.Listener.Close()
}

func (c *connection) Close() error {
	err := c.Conn.Close()

	c.once.Do(c.release)

	return err
}
