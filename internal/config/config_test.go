package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	config := `
engine:
  type: "in_memory"
network:
  address: "localhost:3223"
  max_connections: 100
  max_message_size: "4KB"
  idle_timeout: 5m
logging:
  level: "debug"
  output: "output.log"
  `
	r := strings.NewReader(config)

	viper.SetConfigType("yaml")
	err := viper.ReadConfig(r)
	require.NoError(t, err)

	cfg := Server{}
	err = viper.Unmarshal(&cfg)
	require.NoError(t, err)
	assert.Equal(t, defaultCfg, cfg)
}
