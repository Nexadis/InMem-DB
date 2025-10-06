package tcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"inmem-db/internal/config"

	"golang.org/x/sync/errgroup"
)

type Server struct {
	cfg config.Network

	newHandler HandlerFactory
}

type HandlerFactory func(r io.Reader, w io.Writer) Starter

type Starter interface {
	Start(ctx context.Context) error
}

var DefaultConfig = config.Network{
	MaxMsgSize:     "4KB",
	IdleTimeout:    time.Second * 3,
	MaxConnections: 100,
}

func NewServer(cfg config.Network, newHandler HandlerFactory) *Server {
	return &Server{
		cfg:        cfg,
		newHandler: newHandler,
	}
}

func (s *Server) Start(ctx context.Context) error {
	l, err := net.Listen("tcp", s.cfg.Address)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer l.Close()
	slog.InfoContext(ctx, "start server", slog.String("addr", s.cfg.Address))

	grp, ctx := errgroup.WithContext(ctx)
	grp.SetLimit(s.cfg.MaxConnections)

	go func() {
		<-ctx.Done()
		l.Close()
		err := grp.Wait()
		if err != nil {
			slog.Error("tcp group wait", slog.String("error", err.Error()))
		}
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				slog.Info("server closed")
				return nil
			}
			return fmt.Errorf("accept: %w", err)
		}
		slog.Info("new connection", slog.String("addr", conn.RemoteAddr().String()))

		grp.Go(func() error {
			defer func() {
				if err := recover(); err != nil {
					slog.ErrorContext(ctx, "recover tcp handler", slog.Any("panic", err))
				}
			}()

			return s.handleConn(ctx, conn)
		})
	}
}

func (s *Server) newConn(ctx context.Context, conn net.Conn) error {
	handler := s.newHandler(conn, conn)
	return handler.Start(ctx)
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) error {
	defer func() {
		slog.InfoContext(ctx, "close connection", slog.String("addr", conn.RemoteAddr().String()))
		conn.Close()
	}()

	idled := withIdle(conn, s.cfg.IdleTimeout)

	err := s.newConn(ctx, idled)

	var netErr *net.OpError
	if err != nil && errors.As(err, &netErr) {
		if netErr.Timeout() {
			slog.ErrorContext(ctx, "timeout", slog.String("addr", conn.RemoteAddr().String()))
			return nil
		}
	}

	return err
}
