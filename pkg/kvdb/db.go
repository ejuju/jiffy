package kvdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type DB struct {
	r        *os.File
	w        *os.File
	offset   int
	memstate *memstate
}

func NewDB(fpath string) (*DB, error) {
	db := &DB{}
	var err error

	// Open read-only and write-only file descriptors.
	db.r, db.w, err = openFileRW(fpath)
	if err != nil {
		return nil, err
	}

	// Reconstruct mem-state from datafile and set offset.
	db.offset, db.memstate, err = readMemstate(db.r)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) Close() error {
	if err := db.r.Close(); err != nil {
		return err
	}
	if err := db.w.Close(); err != nil {
		return err
	}
	return nil
}

func (db *DB) Put(k, v []byte) error {
	return db.writeRow(opPut, k, v)
}

func (db *DB) Delete(k []byte) error {
	if !db.Has(k) {
		return ErrKeyNotFound
	}
	return db.writeRow(opDelete, k, nil)
}

func (db *DB) writeRow(opCode byte, k, v []byte) error {
	// Encode row
	row, err := encodeRow(opCode, k, v)
	if err != nil {
		return err
	}

	// Write row
	offsetBeforeWrite := db.offset
	n, err := db.w.Write(row)
	if err != nil {
		return err
	}
	db.offset += n

	// Record new row (in-memory) and increment offset
	if opCode == opPut {
		db.memstate.set(k, &keyref{index: offsetBeforeWrite, width: len(row)})
	} else if opCode == opDelete {
		db.memstate.delete(k)
	}

	return nil
}

var ErrKeyNotFound = errors.New("key not found")

func (db *DB) Get(k []byte) ([]byte, error) {
	ref := db.memstate.get(k)
	if ref == nil {
		return nil, ErrKeyNotFound
	}
	return db.readRefValue(ref, len(k)) // Found ref, read row from file and extract value from row.
}

func (db *DB) readRefValue(ref *keyref, keyLength int) ([]byte, error) {
	row := make([]byte, ref.width)
	_, err := db.r.ReadAt(row, int64(ref.index))
	if err != nil {
		return nil, err
	}
	return row[6+keyLength : len(row)-1], nil
}

func (db *DB) Has(key []byte) bool { return db.memstate.get(key) != nil }

func (db *DB) Count() int { return db.memstate.count }

func (db *DB) ForEachWithPrefix(prefix []byte, callback func(k []byte) bool) {
	db.memstate.scanPrefixWhile(prefix, func(k []byte, _ *keyref) bool { return callback(k) })
}

func (db *DB) Compact() error {
	compactedR, compactedW, err := openFileRW(db.r.Name() + ".compacting")
	if err != nil {
		return err
	}
	compactedWritten := 0
	compactedMemstate := newMemstate()

	db.memstate.scanPrefixWhile([]byte{}, func(k []byte, ref *keyref) bool {
		row := make([]byte, ref.width)
		_, err = db.r.ReadAt(row, int64(ref.index)) // read whole row
		if err != nil {
			err = fmt.Errorf("read row: %w", err)
			return false
		}
		var n int
		offsetBeforeWrite := compactedWritten
		n, err = compactedW.Write(row)
		if err != nil {
			err = fmt.Errorf("write row: %w", err)
			return false
		}
		compactedWritten += n
		compactedMemstate.set(k, &keyref{index: offsetBeforeWrite, width: len(row)})
		return true
	})
	if err != nil {
		return err
	}

	// Replace database file with compacted file (atomic)
	err = os.Rename(compactedR.Name(), db.r.Name())
	if err != nil {
		return fmt.Errorf("replace old datafile with compacted: %w", err)
	}

	// Swap database internals to new compacted file
	db.r, db.w = compactedR, compactedW
	db.memstate = compactedMemstate
	db.offset = compactedWritten
	return nil
}

const (
	opPut    = '='
	opDelete = '!'
)

func isValidOpcode(opcode byte) bool { return opcode == opPut || opcode == opDelete }

var (
	ErrKeyTooLong   = errors.New("key exceeds maximum length (255 bytes)")
	ErrValueTooLong = errors.New("value exceeds maximum length (4 GiB)")
)

const (
	MaxKeyLength   = 1<<8 - 1
	MaxValueLength = 1<<32 - 1
)

func encodeRow(op byte, k []byte, v []byte) ([]byte, error) {
	if len(k) > MaxKeyLength {
		return nil, ErrKeyTooLong
	}
	if len(v) > MaxValueLength {
		return nil, ErrValueTooLong
	}
	var row []byte
	row = append(row, op)                                    // Op
	row = append(row, uint8(len(k)))                         // Key-length as uint8
	row = binary.BigEndian.AppendUint32(row, uint32(len(v))) // Value-length as big-endian uint32
	row = append(row, k...)                                  // Key
	row = append(row, v...)                                  // Value
	row = append(row, '\n')                                  // Trailing LF
	return row, nil
}

func readMemstate(r *os.File) (int, *memstate, error) {
	bufr := bufio.NewReader(r)
	read := 0
	t := newMemstate()

	// Read row by row until EOF or error
	for {
		rowStartOffset := read

		// Read first character of row
		opcode, err := bufr.ReadByte()
		if errors.Is(err, io.EOF) {
			break // EOF is only expected here
		}
		if err != nil {
			return read, nil, fmt.Errorf("read opcode: %w", err)
		}
		read++

		// Check opcode
		if !isValidOpcode(opcode) {
			return read, nil, fmt.Errorf("invalid opcode: %q", opcode)
		}

		// Read key-length
		keyLength, err := bufr.ReadByte()
		if err != nil {
			return read, nil, fmt.Errorf("read key-length: %w", err)
		}
		read++

		// Read value-length
		encodedValueLength := make([]byte, 4)
		_, err = io.ReadFull(bufr, encodedValueLength)
		if err != nil {
			return read, nil, fmt.Errorf("read value-length: %w", err)
		}
		read += len(encodedValueLength)
		valueLength := binary.BigEndian.Uint32(encodedValueLength)

		// Read key
		key := make([]byte, keyLength)
		_, err = io.ReadFull(bufr, key)
		if err != nil {
			return read, nil, fmt.Errorf("read key: %w", err)
		}
		read += len(key)

		// Consume value
		value := make([]byte, valueLength)
		_, err = io.ReadFull(bufr, value)
		if err != nil {
			return read, nil, fmt.Errorf("read value: %w", err)
		}
		read += len(value)

		// Consume LF and continue
		lf, err := bufr.ReadByte()
		if err != nil {
			return read, nil, fmt.Errorf("reading trailing LF: %w", err)
		}
		read++
		if lf != '\n' {
			return read, nil, fmt.Errorf("last row character is not LF: %q", lf)
		}

		// Delete-row: delete the reference from the mem-state to erase previously set reference.
		if opcode == opDelete {
			t.delete(key)
			continue // go to next row
		}

		// Record the key and its corresponding row location on file.
		t.set(key, &keyref{index: rowStartOffset, width: read - rowStartOffset})
	}
	return read, t, nil
}

func openFileRW(fpath string) (*os.File, *os.File, error) {
	r, err := os.OpenFile(fpath, os.O_RDONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, nil, err
	}
	w, err := os.OpenFile(fpath, os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		return nil, nil, err
	}
	return r, w, nil
}
