package jiffy

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

type Op struct {
	Code  byte
	Key   []byte
	Value []byte
}

const (
	OpCodePut    = byte('=')
	OpCodeDelete = byte('!')
)

func IsValidOpCode(op byte) bool { return op == OpCodePut || op == OpCodeDelete }

var ErrUnknownOpCode = errors.New("unknown op-code")

const (
	OpSuffixChar          = byte('(')
	KeyLengthSuffixChar   = byte(':')
	ValueLengthSuffixChar = byte(')')
	KeyPrefixChar         = byte(' ') // extra character for padding
	KeySuffixChar         = byte(' ')
	ValueSuffixChar       = byte('\n')
)

func Encode(op *Op) []byte {
	var out []byte
	out = append(out, op.Code)
	out = append(out, OpSuffixChar)
	out = append(out, strconv.Itoa(len(op.Key))...)
	out = append(out, KeyLengthSuffixChar)
	out = append(out, strconv.Itoa(len(op.Value))...)
	out = append(out, ValueLengthSuffixChar)
	out = append(out, KeyPrefixChar)
	out = append(out, op.Key...)
	out = append(out, KeySuffixChar)
	out = append(out, op.Value...)
	out = append(out, ValueSuffixChar)
	return out
}

func Parse(bufr *bufio.Reader) (int, *Op, error) {
	read := 0
	op := &Op{}

	// Read op-code
	opCodeAndSuff, err := bufr.ReadBytes(OpSuffixChar)
	read += len(opCodeAndSuff)
	if err != nil {
		return read, nil, fmt.Errorf("read op: %w", err)
	}
	op.Code = opCodeAndSuff[0]
	if !IsValidOpCode(op.Code) {
		return read, nil, fmt.Errorf("%w: %q", ErrUnknownOpCode, op.Code)
	}

	// Read key-length
	keyLengthAndSuff, err := bufr.ReadBytes(KeyLengthSuffixChar)
	read += len(keyLengthAndSuff)
	if err != nil {
		return read, nil, fmt.Errorf("read key-length: %w", err)
	}
	keyLength, err := strconv.Atoi(string(keyLengthAndSuff[:len(keyLengthAndSuff)-1]))
	if err != nil {
		return read, nil, fmt.Errorf("parse key-length: %w", err)
	}

	// Read value-length
	valueLengthAndSuff, err := bufr.ReadBytes(ValueLengthSuffixChar)
	read += len(valueLengthAndSuff)
	if err != nil {
		return read, nil, fmt.Errorf("read value-length: %w", err)
	}
	valueLength, err := strconv.Atoi(string(valueLengthAndSuff[:len(valueLengthAndSuff)-1]))
	if err != nil {
		return read, nil, fmt.Errorf("parse value-length: %w", err)
	}

	// Read key (with prefix and suffix)
	keyAndPrefSuff := make([]byte, 1+keyLength+1)
	n, err := io.ReadFull(bufr, keyAndPrefSuff)
	read += n
	if err != nil {
		return read, nil, fmt.Errorf("read key: %w", err)
	}
	op.Key = keyAndPrefSuff[1 : len(keyAndPrefSuff)-1]

	// Read value
	valueAndSuff := make([]byte, valueLength+1)
	n, err = io.ReadFull(bufr, valueAndSuff)
	read += n
	if err != nil {
		return read, nil, fmt.Errorf("read value: %w", err)
	}
	op.Value = valueAndSuff[:len(valueAndSuff)-1]

	return read, op, nil
}
