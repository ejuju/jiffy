package jiffy

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

type FileFormat interface {
	Encode(Line) ([]byte, error)
	Decode(r *bufio.Reader) (int64, Line, error)
}

type Opcode uint8

const (
	OpPut    Opcode = '+' // create or update a key-value pair
	OpDelete Opcode = '-' // delete and remove from history, erase at next merge
	OpCommit Opcode = '.' // mark the end of a transaction
)

type GroupID byte

const (
	MaxKeyLength   = (1 << 8) - 1
	MaxValueLength = (1 << 32) - 1
)

var (
	ErrKeyTooLong   = errors.New("key too long")
	ErrValueTooLong = errors.New("value too long")
)

func ValideKeyValueLengths(key, value []byte) error {
	klen, vlen := len(key), len(value)
	klenOverflows, vlenOverflows := klen > MaxKeyLength, vlen > MaxValueLength
	switch {
	case klenOverflows && vlenOverflows:
		return fmt.Errorf("%w (%d B / 255 B) and %w (%d B / 4 GiB)", ErrKeyTooLong, klen, ErrValueTooLong, vlen)
	case klenOverflows:
		return fmt.Errorf("%w (%d B / 255 B)", ErrKeyTooLong, len(key))
	case vlenOverflows:
		return fmt.Errorf("%w (%d B / 4 GiB)", ErrValueTooLong, len(value))
	}
	return nil
}

// op + group + at + klen + vlen
const binaryFormatHeaderLength = 1 + 1 + 8 + 1 + 4

type Line struct {
	Op      Opcode
	GroupID GroupID
	At      time.Time
	Key     []byte
	Value   []byte
}

type BinaryFileFormat struct{ ByteOrder binary.ByteOrder }

var DefaultBinaryFileFormat = BinaryFileFormat{ByteOrder: binary.BigEndian}

func (bff BinaryFileFormat) Encode(l Line) ([]byte, error) {
	err := ValideKeyValueLengths(l.Key, l.Value)
	if err != nil {
		return nil, err
	}
	header := [binaryFormatHeaderLength]byte{byte(l.Op), byte(l.GroupID)} // + op + GID
	bff.ByteOrder.PutUint64(header[2:], uint64(l.At.UnixNano()))          // + timestamp
	header[10] = uint8(len(l.Key))                                        // + klen
	bff.ByteOrder.PutUint32(header[11:], uint32(len(l.Value)))            // + vlen
	return append(append(header[:], l.Key...), l.Value...), nil           // + key + value
}

func (bff BinaryFileFormat) Decode(r *bufio.Reader) (int64, Line, error) {
	read := int64(0)
	l := Line{}

	// Read header and end of line (only 2 reads needed)
	header := [binaryFormatHeaderLength]byte{}
	n, err := io.ReadFull(r, header[:])
	read += int64(n)
	if err != nil {
		return read, l, fmt.Errorf("read header: %w", err)
	}
	l.Op, l.GroupID = Opcode(header[0]), GroupID(header[1])
	l.At = time.Unix(0, int64(bff.ByteOrder.Uint64(header[2:])))
	klen, vlen := uint8(header[10]), bff.ByteOrder.Uint32(header[11:])

	// Read till end of line (key + value)
	slotsLength := int(klen) + int(vlen)
	slots := make([]byte, slotsLength)
	n, err = io.ReadFull(r, slots)
	read += int64(n)
	if err != nil {
		return read, l, fmt.Errorf("read slots: %w", err)
	}
	if klen > 0 {
		l.Key = slots[:klen]
	} else {
		l.Key = nil
	}
	if vlen > 0 {
		l.Value = slots[klen:]
	} else {
		l.Value = nil
	}
	return read, l, nil
}

type TextFileFormat struct {
	Base                int
	CharSuffixOp        byte
	CharSuffixGroupID   byte
	CharSuffixTimestamp byte
	CharSuffixKey       byte
	CharSuffixValue     byte
}

var DefaultTextFileFormat = TextFileFormat{
	Base:                10,
	CharSuffixOp:        ' ',
	CharSuffixGroupID:   ' ',
	CharSuffixTimestamp: ' ',
	CharSuffixKey:       ' ',
	CharSuffixValue:     '\n',
}

func (tff TextFileFormat) Encode(l Line) ([]byte, error) {
	err := ValideKeyValueLengths(l.Key, l.Value)
	if err != nil {
		return nil, err
	}
	if byte(l.Op) == tff.CharSuffixOp {
		return nil, fmt.Errorf("op is equal to sentinel suffix %q", tff.CharSuffixOp)
	}
	if byte(l.GroupID) == tff.CharSuffixGroupID {
		return nil, fmt.Errorf("group ID is equal to sentinel suffix %q", tff.CharSuffixGroupID)
	}
	if i := bytes.IndexByte(l.Key, tff.CharSuffixKey); i != -1 {
		return nil, fmt.Errorf("key contains sentinel suffix %q at index %d", tff.CharSuffixKey, i)
	}
	if i := bytes.IndexByte(l.Value, tff.CharSuffixValue); i != -1 {
		return nil, fmt.Errorf("value contains sentinel suffix %q at index %d", tff.CharSuffixValue, i)
	}
	b := []byte{byte(l.Op), tff.CharSuffixOp, byte(l.GroupID), tff.CharSuffixGroupID} // + op-suffix + group-suffix
	b = append(b, l.At.Format(time.RFC3339)...)                                       // + timestamp
	b = append(b, tff.CharSuffixTimestamp)                                            // + timestamp-suffix
	b = append(b, l.Key...)                                                           // + key
	b = append(b, tff.CharSuffixKey)                                                  // + key-suffix
	b = append(b, l.Value...)                                                         // + value
	b = append(b, tff.CharSuffixValue)                                                // + value-suffix
	return b, nil
}

func (tff TextFileFormat) Decode(r *bufio.Reader) (int64, Line, error) {
	read := int64(0)
	l := Line{}

	// Read op
	opAndSuffix, err := r.ReadBytes(tff.CharSuffixOp)
	read += int64(len(opAndSuffix))
	if err != nil {
		return read, l, fmt.Errorf("read op: %w", err)
	}
	l.Op = Opcode(opAndSuffix[0])

	// Read group ID
	gidAndSuffix, err := r.ReadBytes(tff.CharSuffixGroupID)
	read += int64(len(gidAndSuffix))
	if err != nil {
		return read, l, fmt.Errorf("read group ID: %w", err)
	}
	l.GroupID = GroupID(gidAndSuffix[0])

	// Read timestamp
	tsAndSuffix, err := r.ReadBytes(tff.CharSuffixTimestamp)
	read += int64(len(tsAndSuffix))
	if err != nil {
		return read, l, fmt.Errorf("read timestamp: %w", err)
	}
	l.At, err = time.Parse(time.RFC3339, string(tsAndSuffix[:len(tsAndSuffix)-1]))
	if err != nil {
		return read, l, fmt.Errorf("parse timestamp: %w", err)
	}

	// Read key
	keyAndSuffix, err := r.ReadBytes(tff.CharSuffixKey)
	read += int64(len(keyAndSuffix))
	if err != nil {
		return read, l, fmt.Errorf("read key: %w", err)
	}
	key := keyAndSuffix[:len(keyAndSuffix)-1]
	if len(key) > 0 {
		l.Key = key
	} else {
		l.Key = nil
	}

	// Read value
	valueAndSuffix, err := r.ReadBytes(tff.CharSuffixValue)
	read += int64(len(valueAndSuffix))
	if err != nil {
		return read, l, fmt.Errorf("read value: %w", err)
	}
	value := valueAndSuffix[:len(valueAndSuffix)-1]
	if len(value) > 0 {
		l.Value = value
	} else {
		l.Value = nil
	}
	return read, l, nil
}
