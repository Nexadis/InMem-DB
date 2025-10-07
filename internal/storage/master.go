package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"inmem-db/internal/server/tcp"
	"inmem-db/internal/storage/wal"
	"inmem-db/internal/storage/wal/decode"
	"inmem-db/internal/storage/wal/encode"
)

type SegmentsGetter interface {
	SegmentsAfter(id int64) []wal.Segment
}

func NewMasterServer(addr string, segmenter SegmentsGetter) *tcp.Server {
	cfg := tcp.DefaultConfig
	cfg.Address = addr

	server := tcp.NewServer(cfg, senderFactory(segmenter))
	return server
}

func senderFactory(segmenter SegmentsGetter) tcp.HandlerFactory {
	return func(r io.Reader, w io.Writer) tcp.Starter {
		return &sender{
			input:     r,
			output:    w,
			segmenter: segmenter,
		}
	}
}

type sender struct {
	input  io.Reader
	output io.Writer

	segmenter SegmentsGetter
}

func (sender *sender) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		afterID, err := decode.ReadID(sender.input)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("read input: %w", err)
		}
		slog.DebugContext(ctx, "get message from slave", slog.Int64("after_id", afterID))

		err = sender.sendSegments(afterID)
		if err != nil {
			slog.ErrorContext(ctx, "send segments", slog.String("error", err.Error()))
		}

	}
}

func (sender *sender) sendSegments(id int64) error {
	segments := sender.segmenter.SegmentsAfter(id)
	err := encode.WriteSize(sender.output, uint32(len(segments)))
	if err != nil {
		return fmt.Errorf("write size: %w", err)
	}
	err = encodeSegments(sender.output, segments)
	if err != nil {
		return err
	}
	return nil
}

func encodeSegments(w io.Writer, segments []wal.Segment) error {
	for _, segment := range segments {
		err := wal.EncodeSegment(w, segment)
		if err != nil {
			return fmt.Errorf("encode segment: %w", err)
		}
	}
	return nil
}
