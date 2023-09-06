package jiffy

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

type Opcode uint8

const (
	OpPut    Opcode = iota // create or update a key-value pair
	OpDelete               // delete and remove from history, erase at next merge
)

const (
	MaxKeyLength   = math.MaxUint8
	MaxValueLength = math.MaxUint32
)

const lineHeaderLength = 1 + 1 + 4 // op (uint8) + len1 (uint8) + len2 (uint32)

type Line struct {
	Op    Opcode
	Key   []byte
	Value []byte
}

func (l Line) MarshalBinary() ([]byte, error) {
	if len(l.Key) > MaxKeyLength {
		return nil, fmt.Errorf("key of length %d exceeds maximum length %d", len(l.Key), MaxKeyLength)
	}
	if len(l.Value) > MaxValueLength {
		return nil, fmt.Errorf("value of length %d exceeds maximum length %d", len(l.Value), MaxValueLength)
	}

	// Encode header
	header := [lineHeaderLength]byte{byte(l.Op), uint8(len(l.Key))}
	binary.BigEndian.PutUint32(header[2:], uint32(len(l.Value)))
	// Append key and value
	return append(append(header[:], l.Key...), l.Value...), nil
}

func (l *Line) ReadFrom(r io.Reader) (int64, error) {
	read := int64(0)

	// Read header and end of line (only 2 reads needed)
	header := [lineHeaderLength]byte{}
	n, err := io.ReadFull(r, header[:])
	read += int64(n)
	if err != nil {
		return read, fmt.Errorf("read header: %w", err)
	}
	op, klen := Opcode(header[0]), uint8(header[1])
	vlen := binary.BigEndian.Uint32(header[2:])

	// Read till end of line (key + value)
	slotsLength := int(klen) + int(vlen)
	slots := make([]byte, slotsLength)
	n, err = io.ReadFull(r, slots)
	read += int64(n)
	if err != nil {
		return read, fmt.Errorf("read slots: %w", err)
	}

	// Mutate line on success
	l.Op = op
	l.Key, l.Value = slots[:klen], slots[klen:]
	return read, nil
}
