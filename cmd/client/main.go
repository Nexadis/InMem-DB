package main

import (
	"context"
	"log"
	"os"

	"inmem-db/internal/client"
	"inmem-db/internal/config"
)

func main() {
	cfg := config.ParseFlags()
	c := client.New(cfg, os.Stdin, os.Stdout)
	err := c.Start(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}
