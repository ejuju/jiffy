package jiffy

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// File holds the in-memory state of a linefile and wraps operations on the underlying file.
type File struct {
	mu         sync.RWMutex
	fpath      string          // Underlying file's path
	fsize      int64           // Current file size (= write offset)
	ffmt       FileFormat      // File encoding format
	r, w       *os.File        // OS file handlers (for reads and writes)
	memidxs    [256]*memindex  // Collections (= ordered-maps of key-value pairs)
	numBuckets map[GroupID]int // Collections' number of hashtable buckets (seperate chaining)
}

// Open opens a file and scans it to restore the memstate.
func Open(fpath string, ffmt FileFormat, numBuckets map[GroupID]int) (*File, error) {
	if fpath == "" {
		return nil, errors.New("missing file path")
	}
	if ffmt == nil {
		ffmt = DefaultTextFileFormat
	}
	f := &File{fpath: fpath, ffmt: ffmt, numBuckets: numBuckets}
	err := f.initMemstate()
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (f *File) Close() error {
	rErr, wErr := f.r.Close(), f.w.Close()
	if hasRErr, hasWErr := rErr != nil, wErr != nil; hasRErr || hasWErr {
		return fmt.Errorf("close files: (failed r=%v/w=%v) %w, %w", hasRErr, hasWErr, rErr, wErr)
	}
	return nil
}

func (f *File) initMemstate() error {
	if f.r != nil && f.w != nil {
		err := f.Close() // close open file descriptors if any
		if err != nil {
			return fmt.Errorf("close open file descriptors: %w", err)
		}
	}

	// Open file descriptors
	var err error
	f.r, err = os.OpenFile(f.fpath, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("open or create read-only file: %w", err)
	}
	f.w, err = os.OpenFile(f.fpath, os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("open write-only file: %w", err)
	}

	// Rebuild memstate
	f.memidxs = [256]*memindex{}
	for cID, cNumBuckets := range f.numBuckets {
		f.memidxs[cID] = newMemindex(cNumBuckets)
	}
	f.fsize = 0
	bufr := bufio.NewReader(f.r)
	for {
		lineStart := f.fsize
		lineLength, l, err := f.ffmt.Decode(bufr)
		f.fsize += lineLength
		if errors.Is(err, io.EOF) {
			if lineLength > 0 {
				f.mustTruncateTailCorruption(lineStart) // We reached EOF on a corrupted row.
			}
			break // We reached the end of the file, all good!
		}
		if err != nil {
			return fmt.Errorf("read row at offset %d: %w", lineStart, err)
		}
		collMemindex := f.memidxs[l.GroupID]
		if collMemindex == nil {
			return fmt.Errorf("collection ID %d not found in memstate", l.GroupID)
		}
		switch l.Op {
		default:
			return fmt.Errorf("illegal op %q at offset %d", l.Op, lineStart)
		case OpDelete:
			collMemindex.delete(l.Key)
		case OpPut:
			collMemindex.put(l.Key, l.At, NewPosition(lineStart, lineLength))
		case OpCommit:
			panic("todo")
		}
	}
	return nil
}
