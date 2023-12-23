package log

import (
	api "github.com/acikgozb/proglog/github.com/acikgozb/api/log_v1"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"testing"
)

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-test")
	defer os.RemoveAll(dir)

	apiRecord := &api.Record{
		Value: []byte("hello world"),
	}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entryWidth * 3

	var baseSegmentOffset uint64 = 16

	segment, segmentError := newSegment(dir, baseSegmentOffset, c)
	require.NoError(t, segmentError)
	require.Equal(t, baseSegmentOffset, segment.baseOffset)
	require.False(t, segment.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		insertedRecordOffset, err := segment.Append(apiRecord)
		require.NoError(t, err)
		require.Equal(t, baseSegmentOffset+1, insertedRecordOffset)

		retrievedRecord, err := segment.Read(insertedRecordOffset)
		require.NoError(t, err)
		require.Equal(t, apiRecord.Value, retrievedRecord.Value)
	}

	_, err := segment.Append(apiRecord)
	require.Equal(t, io.EOF, err)

	//index should be maxed therefore segment should be maxed.
	require.True(t, segment.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(apiRecord.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	segment, err = newSegment(dir, baseSegmentOffset, c)
	require.NoError(t, err)

	//store should be maxed because we changed the maxStoreBytes to inserted record count, which is 3.
	require.True(t, segment.IsMaxed())

	err = segment.Remove()
	require.NoError(t, err)

	segment, err = newSegment(dir, baseSegmentOffset, c)
	require.NoError(t, err)
	//since we removed the segment, the new segment should be empty
	require.False(t, segment.IsMaxed())
}
