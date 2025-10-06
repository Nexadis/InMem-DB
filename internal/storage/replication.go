package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"time"

	"inmem-db/internal/client"
	"inmem-db/internal/config"
	"inmem-db/internal/domain/command"
)

var ErrReadOnly = errors.New("replication is read only")

type replication struct {
	client *client.Client
	ticker *time.Ticker

	recv *io.PipeReader
	send *io.PipeWriter

	wal segmentManager
	e   Engine
}

type segmentManager interface {
	LastSegmentID() int64
	ApplySegments(data []byte) ([]command.Command, error)
}

// TODO:Arch
// Slave:
// 	- запрашивает периодически данные с мастера
// 	- получает новые сегменты
// 	- записывает сегменты на диск, проигрывает на движке

func newReplicationClient(cfg config.Replication, wal segmentManager, e Engine) *replication {
	sendReader, sendWriter := io.Pipe()
	recvReader, recvWriter := io.Pipe()

	client := client.New(
		config.Client{
			Address: cfg.MasterAddress,
		}, sendReader, recvWriter)

	ticker := time.NewTicker(cfg.SyncInterval)
	return &replication{
		client: client,
		ticker: ticker,

		recv: recvReader,
		send: sendWriter,
		wal:  wal,
		e:    e,
	}
}

func (r *replication) Start(ctx context.Context) error {
	go func() {
		defer r.ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				r.Close()
				return
			default:
			}
			select {
			case <-ctx.Done():
				r.Close()
				return
			case <-r.ticker.C:
				err := r.sync(ctx)
				if err != nil {
					slog.ErrorContext(ctx, "sync with master", slog.String("error", err.Error()))
				}
			}
		}
	}()

	return r.client.Start(ctx)
}

func (r *replication) sync(ctx context.Context) error {
	slog.DebugContext(ctx, "sync master")

	lastID := r.wal.LastSegmentID()
	data := makeRequest(lastID)
	_, err := r.send.Write(data)
	if err != nil {
		return fmt.Errorf("write segment id: %w", err)
	}
	size := int32(0)
	err = binary.Read(r.recv, binary.BigEndian, &size)
	if err != nil {
		return fmt.Errorf("read data size : %w", err)
	}

	if size == 0 {
		slog.Debug("nothing to sync")
		return nil
	}

	data = make([]byte, size)
	_, err = r.recv.Read(data)
	if err != nil {
		return fmt.Errorf("read segments: %w", err)
	}

	cmds, err := r.wal.ApplySegments(data)
	if err != nil {
		return fmt.Errorf("apply segments: %w", err)
	}
	for _, cmd := range cmds {
		_, err := r.e.Do(ctx, cmd)
		if err != nil {
			return fmt.Errorf("engine do command while sync: %w", err)
		}
	}

	return nil
}

func (r *replication) Close() {
	_ = r.recv.Close()
	_ = r.send.Close()
}

func makeRequest(id int64) []byte {
	data := strconv.AppendInt([]byte{}, id, 10)
	return data
}
