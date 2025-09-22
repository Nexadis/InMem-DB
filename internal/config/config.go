package config

import (
	"os"
	"path"
	"time"

	"github.com/spf13/viper"
)

const configENV = "CONFIG_FILE"

type Server struct {
	Engine  Engine  `mapstructure:"engine"`
	Network Network `mapstructure:"network"`
	Logging Logging `mapstructure:"logging"`
}

type EngineType string

const (
	EngineTypeMem = "in_memory"
)

type Engine struct {
	Type EngineType `mapstructure:"type"`
}

type Network struct {
	Address     string        `mapstructure:"address"`
	MaxMsgSize  string        `mapstructure:"max_message_size"`
	IdleTimeout time.Duration `mapstructure:"idle_timeout"`

	MaxConnections int `mapstructure:"max_connections"`
}

type Logging struct {
	Level  LogLevel `mapstructure:"level"`
	Output string   `mapstructure:"output"`
}
type LogLevel string

type WAL struct {
	BatchSize    uint          `mapstructure:"flushing_batch_size"`
	BatchTimeout time.Duration `mapstructure:"flushing_batch_timeout"`

	MaxSegmentSize string `mapstructure:"max_segment_size"`
	DataDir        string `mapstructure:"data_directory"`
}

const (
	LevelDebug LogLevel = "debug"
	LevelInfo  LogLevel = "info"
	LevelError LogLevel = "error"
)

var defaultCfg = Server{
	Engine: Engine{Type: EngineTypeMem},
	Network: Network{
		Address:        "localhost:3223",
		MaxConnections: 100,
		MaxMsgSize:     "4KB",
		IdleTimeout:    5 * time.Minute,
	},
	Logging: Logging{
		Level:  LevelDebug,
		Output: "output.log",
	},
}

func MustLoad() Server {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	confPath, ok := os.LookupEnv(configENV)
	if ok {
		name := path.Base(confPath)
		dir := path.Dir(confPath)
		viper.AddConfigPath(dir)
		viper.SetConfigName(name)
	}

	viper.AddConfigPath("configs")

	err := viper.ReadInConfig()
	if err != nil {
		return defaultCfg
	}

	cfg := Server{}
	err = viper.Unmarshal(&cfg)
	if err != nil {
		return defaultCfg
	}
	return cfg
}
