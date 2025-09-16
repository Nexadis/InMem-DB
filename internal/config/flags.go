package config

import "flag"

type Client struct {
	Address string
}

func ParseFlags() Client {
	cfg := Client{}
	flag.StringVar(&cfg.Address, "address", "localhost:3223", "Address of tcp server for connection")
	flag.Parse()
	return cfg
}
