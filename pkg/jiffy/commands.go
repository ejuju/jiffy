package jiffy

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

var ErrCollectionNotFound = errors.New("collection not found")

type ListOp struct {
	List string
	Op   *Op
}

type Writer struct {
	db  *Database
	ops []*ListOp
}

// Return an error in the callback to discard the update.
func (db *Database) ReadWrite(callback func(r *Reader, w *Writer) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	writer := &Writer{db: db}
	reader := &Reader{db: db}
	err := callback(reader, writer)
	if err != nil {
		return err
	}

	// Write all mutations to files and update memstate
	totalWritten := 0
	for _, mutation := range writer.ops {
		df, ok := db.files[mutation.List]
		if !ok && totalWritten > 0 {
			// todo: undo keymap and truncate file
			panic("file corruption")
		} else if !ok {
			return fmt.Errorf("%w: %q", ErrCollectionNotFound, mutation.List)
		}

		b := Encode(mutation.Op)
		n, err := df.w.Write(b)
		df.woffset += n
		totalWritten += n
		if errors.Is(err, io.ErrShortWrite) {
			// todo: undo keymap and truncate file
			panic(fmt.Errorf("file corruption: %w (incomplete write: %d/%d)", err, n, len(b)))
		} else if err != nil && totalWritten > 0 {
			// todo: undo keymap and truncate file
			panic(fmt.Errorf("file corruption: %w", err))
		} else if err != nil && totalWritten == 0 {
			return err // failed first write and no bytes written, no file or keymap corruption
		}
		if err := df.w.Sync(); err != nil {
			// todo: undo keymap and truncate file
			return err
		}

		if mutation.Op.Code == OpCodeDelete {
			if _, ok := df.keymap.get(mutation.Op.Key); !ok {
				continue // no-op if key not found
			}
			df.keymap.delete(mutation.Op.Key)
		} else {
			df.keymap.put(mutation.Op.Key, df.woffset-n)
		}
	}
	return nil
}

func (w *Writer) With(lists ...string) error {
	var err error
	for _, name := range lists {
		if _, ok := w.db.files[name]; ok {
			continue // no-op if already opened
		}
		w.db.files[name], err = openDatafile(w.db.dirpath, name)
		if err != nil {
			return fmt.Errorf("open list %q: %w", name, err)
		}
	}
	return nil
}

func (w *Writer) In(name string) *ListWriter { return &ListWriter{name: name, w: w} }

type ListWriter struct {
	w    *Writer
	name string
}

func (lw *ListWriter) Put(key, value []byte) {
	lw.w.ops = append(lw.w.ops, &ListOp{List: lw.name, Op: &Op{Code: OpCodePut, Key: key, Value: value}})
}

func (lw *ListWriter) Delete(key []byte) {
	lw.w.ops = append(lw.w.ops, &ListOp{List: lw.name, Op: &Op{Code: OpCodeDelete, Key: key}})
}

type Reader struct {
	db *Database
}

func (db *Database) Read(callback func(r *Reader) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	reader := &Reader{db: db}
	return callback(reader)
}

func (r *Reader) NumberOfLists() int { return len(r.db.files) }

func (r *Reader) In(name string) *ListReader {
	df, ok := r.db.files[name]
	if !ok {
		return nil
	}
	return &ListReader{name: name, df: df}
}

type ListReader struct {
	df   *datafile
	name string
}

func (lr *ListReader) Exists(key []byte) bool {
	_, ok := lr.df.keymap.get(key)
	return ok
}

func (lr *ListReader) Get(key []byte) ([]byte, error) {
	roffset, ok := lr.df.keymap.get(key)
	if !ok {
		return nil, nil // key not found returns nil value (no error)
	}
	op, err := lr.readOp(roffset)
	if err != nil {
		return nil, err
	}
	return op.Value, nil
}

func (lr *ListReader) readOp(roffset int) (*Op, error) {
	_, err := lr.df.r.Seek(int64(roffset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek op start: %w", err)
	}
	_, op, err := Parse(bufio.NewReader(lr.df.r))
	if err != nil {
		return nil, fmt.Errorf("parse op: %w", err)
	}
	return op, nil
}

func (lr *ListReader) Walk(prefix []byte, callback func(key []byte) (bool, error)) error {
	return lr.df.keymap.walk(prefix, func(k []byte, _ int) (bool, error) { return callback(k) })
}

func (lr *ListReader) WalkWithValue(prefix []byte, callback func(key, value []byte) (bool, error)) error {
	return lr.df.keymap.walk(prefix, func(k []byte, roffset int) (bool, error) {
		op, err := lr.readOp(roffset)
		if err != nil {
			return false, err
		}
		return callback(k, op.Value)
	})
}
