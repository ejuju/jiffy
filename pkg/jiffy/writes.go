package jiffy

import (
	"errors"
	"fmt"
	"io"
)

func (f *File) Put(key, value []byte) error {
	offset, length, err := f.append(OpPut, key, value)
	if err != nil {
		return err
	}
	f.midx.put(key, NewPosition(offset, length))
	return nil
}

func (f *File) Delete(key []byte) error {
	_, _, err := f.append(OpDelete, key, nil)
	if err != nil {
		return err
	}
	f.midx.delete(key)
	return nil
}

func (f *File) Sync() error { return f.w.Sync() }

func (f *File) append(opcode Opcode, slot1, slot2 []byte) (int64, int64, error) {
	startOffset := f.fsize
	encoded, err := Line{Op: opcode, Key: slot1, Value: slot2}.MarshalBinary()
	if err != nil {
		return startOffset, 0, err
	}
	n, err := f.w.WriteAt(encoded, int64(f.fsize))
	f.fsize += int64(n)
	if err != nil {
		if errors.Is(err, io.ErrShortWrite) {
			f.mustTruncateTailCorruption(startOffset)
		}
		return startOffset, int64(n), fmt.Errorf("write new line: %w", err)
	}
	return startOffset, int64(n), nil
}

func (f *File) mustTruncateTailCorruption(truncateAt int64) {
	err := f.w.Truncate(truncateAt)
	if err != nil {
		panic(fmt.Errorf("file tail corruption at offset %d: %w", truncateAt, err))
	}
	f.fsize = truncateAt
	f.initMemstate()
}
