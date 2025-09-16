package client

import (
	"context"
	"fmt"
	"io"
	"net"

	"inmem-db/internal/config"
)

type Client struct {
	cfg config.Client

	input  io.Reader
	output io.Writer
}

func New(cfg config.Client, input io.Reader, output io.Writer) *Client {
	return &Client{
		cfg:    cfg,
		input:  input,
		output: output,
	}
}

func (c *Client) Start(ctx context.Context) error {
	conn, err := net.Dial("tcp", c.cfg.Address)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	go func() {
		_, _ = io.Copy(c.output, conn)
	}()

	_, err = io.Copy(conn, c.input)
	return err
}
