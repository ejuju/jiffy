package jiffy

import (
	"encoding/binary"
	"fmt"
	"io"
)

type Opcode uint8

const (
	OpPut    Opcode = iota // create or update a key-value pair
	OpDelete               // delete and remove from history, erase at next merge
)

type GroupID uint8

const (
	MaxKeyLength      = (1 << 8) - 1
	MaxValueLength    = (1 << 32) - 1
	MaxNumCollections = (1 << 8)
	MaxCollectionID   = MaxNumCollections - 1
)

const LineHeaderLength = 1 + 1 + 1 + 4 // op (uint8) + group (uint8) + len1 (uint8) + len2 (uint32)

type Line struct {
	Op      Opcode
	GroupID GroupID
	Key     []byte
	Value   []byte
}

func (l Line) MarshalBinary() ([]byte, error) {
	if len(l.Key) > MaxKeyLength {
		return nil, fmt.Errorf("key of length %d exceeds maximum length %d", len(l.Key), MaxKeyLength)
	}
	if len(l.Value) > MaxValueLength {
		return nil, fmt.Errorf("value of length %d exceeds maximum length %d", len(l.Value), MaxValueLength)
	}

	// Encode header
	header := [LineHeaderLength]byte{byte(l.Op), byte(l.GroupID), uint8(len(l.Key))}
	binary.BigEndian.PutUint32(header[3:], uint32(len(l.Value)))

	// Append key and value
	return append(append(header[:], l.Key...), l.Value...), nil
}

func (l *Line) ReadFrom(r io.Reader) (int64, error) {
	read := int64(0)

	// Read header and end of line (only 2 reads needed)
	header := [LineHeaderLength]byte{}
	n, err := io.ReadFull(r, header[:])
	read += int64(n)
	if err != nil {
		return read, fmt.Errorf("read header: %w", err)
	}
	op, collectionID, klen := Opcode(header[0]), GroupID(header[1]), uint8(header[2])
	vlen := binary.BigEndian.Uint32(header[3:])

	// Read till end of line (key + value)
	slotsLength := int(klen) + int(vlen)
	slots := make([]byte, slotsLength)
	n, err = io.ReadFull(r, slots)
	read += int64(n)
	if err != nil {
		return read, fmt.Errorf("read slots: %w", err)
	}
	var key, value []byte
	if klen > 0 {
		key = slots[:klen]
	} else {
		key = nil
	}
	if vlen > 0 {
		value = slots[klen:]
	} else {
		value = nil
	}

	// Mutate line on success
	l.Op = op
	l.GroupID = collectionID
	l.Key, l.Value = key, value
	return read, nil
}
