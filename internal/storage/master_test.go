package storage

import (
	"testing"
	"time"

	"inmem-db/internal/config"
	"inmem-db/internal/domain/command"
	"inmem-db/internal/storage/engine"
	"inmem-db/internal/storage/wal"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/nettest"
)

const syncTime = time.Millisecond * 10

func TestMasterReplication_SlaveGetCopy(t *testing.T) {
	t.Parallel()
	type res struct {
		s   string
		err error
	}

	type test struct {
		masterCMD command.Command
		slaveCMD  command.Command
		slaveRes  res
	}

	tests := map[string]test{
		"set value": {
			masterCMD: command.Command{
				Type: command.CommandSET,
				Name: "test_name01928",
				Set: command.SetArgs{
					Value: "test_value90812",
				},
			},
			slaveCMD: command.Command{
				Type: command.CommandGET,
				Name: "test_name01928",
			},
			slaveRes: res{
				s:   "test_value90812",
				err: nil,
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			help := setupTest(t)

			_, err := help.master.Do(ctx, test.masterCMD)
			require.NoError(t, err)

			select {
			case <-time.After(syncTime):
			case <-ctx.Done():
			}

			s, err := help.slave.Do(ctx, test.slaveCMD)
			assert.Equal(t, test.slaveRes.s, s)
			assert.Equal(t, test.slaveRes.err, err)
		})
	}
}

type testHelper struct {
	master *Storage
	slave  *Storage
}

func setupTest(t *testing.T) testHelper {
	t.Helper()
	th := testHelper{}
	th.newMaster(t)

	ctx := t.Context()
	go th.master.Start(ctx)
	go th.slave.Start(ctx)

	return th
}

func (th *testHelper) newMaster(t *testing.T) {
	masterEngine := engine.New()
	walConfig := config.WAL{
		BatchSize:      5,
		BatchTimeout:   syncTime,
		MaxSegmentSize: "10MB",
		DataDir:        t.TempDir(),
	}

	w, err := wal.New(walConfig)
	require.NoError(t, err)

	l, err := nettest.NewLocalListener("tcp")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	masterServer := NewMasterServer(addr, w)

	th.master = New(masterEngine, w, WithMasterServer(masterServer))
	th.newSlave(t, addr)
}

func (th *testHelper) newSlave(t *testing.T, masterAddress string) {
	e := engine.New()
	walConfig := config.WAL{
		BatchSize:      5,
		BatchTimeout:   syncTime,
		MaxSegmentSize: "10MB",
		DataDir:        t.TempDir(),
	}

	w, err := wal.New(walConfig)
	require.NoError(t, err)

	cfg := config.Replication{
		ReplicaType:   config.SlaveReplica,
		MasterAddress: masterAddress,
		SyncInterval:  syncTime / 2,
	}

	client := NewReplicationClient(cfg, w, e)
	th.slave = New(e, w, WithReplicationClient(client))
}
