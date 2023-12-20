package server

import (
	"fmt"
	"sync"
)

type Log struct {
	mu      sync.Mutex
	records []Record
}

type Record struct {
	Offset uint64
	Value  []byte
}

func NewLog() *Log {
	return &Log{}
}

func (log *Log) Append(record Record) (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	record.Offset = uint64(len(log.records))
	log.records = append(log.records, record)

	return record.Offset, nil
}

func (log *Log) Read(offset uint64) (Record, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	if offset >= uint64(len(log.records)) {
		return Record{}, ErrOffsetNotFound
	}

	return log.records[offset], nil
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")
