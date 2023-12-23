package log

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	config := Config{}
	config.Segment.MaxIndexBytes = 1024

	index, indexErr := newIndex(f, config)
	require.NoError(t, indexErr)

	_, _, readErr := index.Read(-1)
	require.Error(t, readErr)
	require.Equal(t, f.Name(), index.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, entry := range entries {
		writeErr := index.Write(entry.Off, entry.Pos)
		require.NoError(t, writeErr)

		_, pos, readErr := index.Read(int64(entry.Off))
		require.NoError(t, readErr)
		require.Equal(t, entry.Pos, pos)
	}
}
