package config

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

const configENV = "CONFIG_FILE"

type EnvType string

const (
	EnvDev  EnvType = "dev"
	EnvProd EnvType = "prod"
)

type Config struct {
	Env EnvType `yaml:"env,omitempty"`
}

func MustLoad() Config {
	cfg := Config{
		Env: EnvProd,
	}

	path, ok := os.LookupEnv(configENV)
	if !ok {
		log.Fatalf("env %s is not set", configENV)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(content, &cfg)
	if err != nil {
		log.Fatal(err)
	}

	return cfg
}
