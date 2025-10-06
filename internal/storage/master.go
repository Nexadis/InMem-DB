package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"

	"inmem-db/internal/server/tcp"
)

type SegmentsGetter interface {
	AfterSegments(id int64) ([]byte, error)
}

// TODO:
// Master:
//	- читает всё что есть с диска
//	- отдаёт все сегменты после какого-то момента (ID > ?)
//	- отправляет данные в replication, с запрашиваемого момента

func newMasterServer(addr string, segmenter SegmentsGetter) *tcp.Server {
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
	afterID := make([]byte, 4)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		_, err := sender.input.Read(afterID)
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("read input: %w", err)
		}
		afterID = bytes.Trim(afterID, string([]byte{0}))

		slog.DebugContext(ctx, "get message from slave", slog.String("line", string(afterID)))

		err = sender.sendSegments(string(afterID))
		if err != nil {
			slog.ErrorContext(ctx, "send segments", slog.String("error", err.Error()))
		}

	}
}

func (sender *sender) sendSegments(line string) error {
	id, err := parseID(line)
	if err != nil {
		return fmt.Errorf("parse segment id: %w", err)
	}

	data, err := sender.segmenter.AfterSegments(id)
	if err != nil {
		return fmt.Errorf("get segments: %w", err)
	}
	size := int32(len(data))
	err = binary.Write(sender.output, binary.BigEndian, &size)
	if err != nil {
		return fmt.Errorf("write size: %w", err)
	}

	_, err = sender.output.Write(data)
	if err != nil {
		return fmt.Errorf("write segments: %w", err)
	}
	return nil
}

func parseID(line string) (int64, error) {
	id, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid id: %w", err)
	}
	return id, nil
}
