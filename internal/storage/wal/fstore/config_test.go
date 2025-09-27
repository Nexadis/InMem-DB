package fstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSize(t *testing.T) {
	t.Parallel()

	type test struct {
		sizeStr string
		size    uint64
		wantErr bool
	}

	tests := map[string]test{
		"upper size": {
			sizeStr: "10MB",
			size:    1 << 20 * 10,
			wantErr: false,
		},
		"lower size": {
			sizeStr: "500b",
			size:    500,
			wantErr: false,
		},
		"invalid size scale": {
			sizeStr: "10Amb",
			wantErr: true,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, gotErr := parseSize(test.sizeStr)
			if test.wantErr {
				assert.Error(t, gotErr)
				return
			}
			assert.NoError(t, gotErr)
			assert.Equal(t, test.size, got)
		})
	}
}
