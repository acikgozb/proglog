package log

import (
	"github.com/tysonmote/gommap"
	"io"
	"os"
)

var (
	offsetWidth   uint64 = 4
	positionWidth uint64 = 8
	entryWidth           = offsetWidth + positionWidth
)

type index struct {
	file             *os.File
	memoryMappedFile gommap.MMap
	size             uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	index := &index{
		file: f,
	}

	fileInfo, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	index.size = uint64(fileInfo.Size())
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if index.memoryMappedFile, err = gommap.Map(
		index.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}

	return index, nil
}

func (idx *index) Read(in int64) (out uint32, pos uint64, err error) {
	//if there's nothing to read, return end of file error.
	if idx.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((idx.size / entryWidth) - 1) // if caller wants to read the previous entry, determine the offset for it.
	} else {
		out = uint32(in)
	}

	// pos is current position inside the file based on given inputOffset (in).
	pos = uint64(out) * entryWidth
	//if we can't read an entry starting from the position, return err.
	if idx.size < pos+entryWidth {
		return 0, 0, io.EOF
	}

	//returns the offset by reading between current pos and pos+offsetWidth
	out = enc.Uint32(idx.memoryMappedFile[pos : pos+offsetWidth])
	//returns the position of the read entry
	pos = enc.Uint64(idx.memoryMappedFile[pos+offsetWidth : pos+entryWidth])
	return out, pos, nil
}

func (idx *index) Write(offset uint32, position uint64) error {
	// If we don't have enough space in file, we can't add new entry, return EOF error.
	if uint64(len(idx.memoryMappedFile)) < idx.size+entryWidth {
		return io.EOF
	}

	// add new offset to memoryMappedFile - which should start from the end of the index size and takes offsetWidth space.
	enc.PutUint32(idx.memoryMappedFile[idx.size:idx.size+offsetWidth], offset)
	// add the position of new entry to memoryMappedFile - which takes space between offsetWidth and entryWidth.
	enc.PutUint64(idx.memoryMappedFile[idx.size+offsetWidth:idx.size+entryWidth], position)

	// increment the index position for the next write operation.
	idx.size += entryWidth
	return nil
}

func (idx *index) Name() string {
	return idx.file.Name()
}

func (idx *index) Close() error {
	if err := idx.memoryMappedFile.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := idx.file.Sync(); err != nil {
		return err
	}

	if err := idx.file.Truncate(int64(idx.size)); err != nil {
		return err
	}

	return idx.file.Close()
}
