package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"inmem-db/internal/client"
	"inmem-db/internal/config"
	"inmem-db/internal/domain/command"
	"inmem-db/internal/storage/wal"
	"inmem-db/internal/storage/wal/decode"
	"inmem-db/internal/storage/wal/encode"
)

var ErrReadOnly = errors.New("replication is read only")

type replicationClient struct {
	client *client.Client
	ticker *time.Ticker

	recv *io.PipeReader
	send *io.PipeWriter

	wal segmentManager
	e   Engine

	cleanup func()
}

type segmentManager interface {
	LastSegmentID() int64
	SaveSegment(segment wal.Segment) error
}

func NewReplicationClient(cfg config.Replication, wal segmentManager, e Engine) *replicationClient {
	sendReader, sendWriter := io.Pipe()
	recvReader, recvWriter := io.Pipe()

	client := client.New(
		config.Client{
			Address: cfg.MasterAddress,
		}, sendReader, recvWriter)

	ticker := time.NewTicker(cfg.SyncInterval)
	cleanup := func() {
		recvReader.Close()
		recvWriter.Close()
		sendReader.Close()
		sendWriter.Close()
	}

	return &replicationClient{
		client: client,
		ticker: ticker,

		recv: recvReader,
		send: sendWriter,
		wal:  wal,
		e:    e,

		cleanup: cleanup,
	}
}

func (r *replicationClient) Start(ctx context.Context) error {
	go func() {
		defer r.ticker.Stop()
		defer r.cleanup()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			select {
			case <-ctx.Done():
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

func (r *replicationClient) sync(ctx context.Context) error {
	slog.DebugContext(ctx, "sync master")

	segments, err := r.loadNewSegments(ctx)
	if err != nil {
		return fmt.Errorf("load new segments: %w", err)
	}

	if len(segments) == 0 {
		return nil
	}

	slog.DebugContext(ctx, "got new segments", slog.Int("length", len(segments)))

	err = r.applySegments(ctx, segments)
	if err != nil {
		return fmt.Errorf("apply segments: %w", err)
	}

	return nil
}

func (r *replicationClient) countNewSegments(_ context.Context) (uint32, error) {
	lastID := r.wal.LastSegmentID()
	err := encode.WriteID(r.send, lastID)
	if err != nil {
		return 0, fmt.Errorf("write segment id: %w", err)
	}

	count, err := decode.ReadSize(r.recv)
	if err != nil {
		return 0, fmt.Errorf("read segments size : %w", err)
	}
	return count, nil
}

func (r *replicationClient) loadNewSegments(ctx context.Context) ([]wal.Segment, error) {
	count, err := r.countNewSegments(ctx)
	if err != nil {
		return nil, fmt.Errorf("count new segments: %w", err)
	}

	if count == 0 {
		return []wal.Segment{}, nil
	}

	segments := make([]wal.Segment, count)
	err = decodeSegments(r.recv, segments)
	if err != nil {
		return nil, err
	}
	return segments, nil
}

func (r *replicationClient) applySegments(ctx context.Context, segments []wal.Segment) error {
	for _, s := range segments {
		err := r.wal.SaveSegment(s)
		if err != nil {
			return fmt.Errorf("save segment: %w", err)
		}
		cmds := wal.SegmentCommands(s)
		err = doCommands(ctx, r.e, cmds)
		if err != nil {
			return fmt.Errorf("do segment commands: %w", err)
		}
	}
	return nil
}

func doCommands(ctx context.Context, e Engine, cmds []command.Command) error {
	for _, cmd := range cmds {
		_, err := e.Do(ctx, cmd)
		if err != nil {
			return fmt.Errorf("engine do while sync: %w", err)
		}
	}
	return nil
}
