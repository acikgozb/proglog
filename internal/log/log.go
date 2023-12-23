package log

import (
	"fmt"
	api "github.com/acikgozb/proglog/github.com/acikgozb/api/log_v1"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	log := &Log{
		Dir:    dir,
		Config: c,
	}

	return log, log.setup()
}

func (log *Log) setup() error {
	files, err := os.ReadDir(log.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, file := range files {
		offsetStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		offset, _ := strconv.ParseUint(offsetStr, 10, 0)
		baseOffsets = append(baseOffsets, offset)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err = log.newSegment(baseOffsets[i]); err != nil {
			return err
		}

		i++
	}

	if log.segments == nil {
		if err = log.newSegment(log.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}

	return nil
}

func (log *Log) Append(record *api.Record) (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	off, err := log.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if log.activeSegment.IsMaxed() {
		err = log.newSegment(off + 1)
	}

	return off, err
}

func (log *Log) Read(offset uint64) (*api.Record, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()

	var segment *segment
	for _, currentSegment := range log.segments {
		if currentSegment.baseOffset <= offset && offset < currentSegment.nextOffset {
			segment = currentSegment
			break
		}
	}

	if segment == nil || segment.nextOffset <= offset {
		return nil, fmt.Errorf("offset out of range: %d", offset)
	}

	return segment.Read(offset)
}

func (log *Log) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	for _, segment := range log.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (log *Log) Remove() error {
	if err := log.Close(); err != nil {
		return err
	}

	return os.RemoveAll(log.Dir)
}

func (log *Log) Reset() error {
	if err := log.Remove(); err != nil {
		return err
	}

	return log.setup()
}

func (log *Log) LowestOffset() (uint64, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()
	return log.segments[0].baseOffset, nil
}

func (log *Log) HighestOffset() (uint64, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()
	offset := log.segments[len(log.segments)-1].nextOffset
	if offset == 0 {
		return 0, nil
	}

	return offset - 1, nil
}

func (log *Log) Truncate(thresholdOffset uint64) error {
	log.mu.Lock()
	defer log.mu.Unlock()

	var truncatedSegments []*segment
	for _, currentSegment := range log.segments {
		if currentSegment.nextOffset < thresholdOffset+1 {
			if err := currentSegment.Remove(); err != nil {
				return err
			}

			continue
		}

		truncatedSegments = append(truncatedSegments, currentSegment)
	}

	log.segments = truncatedSegments
	return nil
}
