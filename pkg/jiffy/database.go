package jiffy

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type Database struct {
	mu      sync.RWMutex
	dirpath string
	files   map[string]*datafile
}

func New(dirpath string) *Database {
	return &Database{dirpath: dirpath, files: map[string]*datafile{}}
}

func (db *Database) Close() []error {
	var errs []error
	for name, f := range db.files {
		err := f.close()
		if err != nil {
			errs = append(errs, fmt.Errorf("close %q: %w", name, err))
		}
	}
	return errs
}

const FileExtension = ".jiffy"

type datafile struct {
	fpath   string
	r, w    *os.File
	woffset int
	keymap  *keymap // key to file offset
}

func openDatafile(dirpath string, name string) (*datafile, error) {
	coll := &datafile{fpath: filepath.Join(dirpath, name+FileExtension), keymap: &keymap{root: &trieNode{}}}
	var err error
	coll.r, coll.w, err = openFileRW(coll.fpath)
	if err != nil {
		return nil, err
	}
	bufr := bufio.NewReader(coll.r)
	for {
		n, op, err := Parse(bufr)
		coll.woffset += n
		if errors.Is(err, io.EOF) && n == 0 {
			break // expected EOF
		}
		if err != nil {
			return nil, fmt.Errorf("%w (at offset %d)", err, coll.woffset)
		}
		if op.Code == OpCodeDelete {
			coll.keymap.delete(op.Key)
			continue
		}
		coll.keymap.put(op.Key, coll.woffset-n)
	}
	return coll, nil
}

func (df *datafile) close() error {
	rerr, werr := df.r.Close(), df.w.Close()
	if rerr != nil || werr != nil {
		return fmt.Errorf("close file (r/w): %w, %w", rerr, werr)
	}
	return nil
}

func openFileRW(fpath string) (*os.File, *os.File, error) {
	r, err := os.OpenFile(fpath, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}
	w, err := os.OpenFile(fpath, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, nil, err
	}
	return r, w, nil
}

type keymap struct{ root *trieNode }

type trieNode struct {
	children [256]*trieNode
	isLeaf   bool
	offset   int
}

func (t *keymap) put(key []byte, offset int) {
	curr := t.root
	for _, char := range key {
		if curr.children[char] == nil {
			curr.children[char] = &trieNode{}
		}
		curr = curr.children[char]
	}
	curr.isLeaf = true
	curr.offset = offset
}

func (t *keymap) delete(key []byte) {
	curr := t.root
	for _, char := range key {
		if curr.children[char] == nil {
			return // no-op if key not found
		}
		curr = curr.children[char]
	}
	curr.isLeaf = false
}

func (t *keymap) get(key []byte) (int, bool) {
	curr := t.root
	for _, char := range key {
		if curr.children[char] == nil {
			return 0, false
		}
		curr = curr.children[char]
	}
	return curr.offset, curr.isLeaf
}

func (t *keymap) walk(prefix []byte, callback func(key []byte, roffset int) (bool, error)) error {
	curr := t.root
	for _, c := range prefix {
		if curr.children[c] == nil {
			return nil
		}
		curr = curr.children[c]
	}
	return curr.walk(prefix, callback)
}

func (n *trieNode) walk(prefix []byte, callback func([]byte, int) (bool, error)) error {
	if n.isLeaf {
		ok, err := callback(prefix, n.offset)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	for c, child := range n.children {
		if child == nil {
			continue
		}
		err := child.walk(append(prefix, byte(c)), callback)
		if err != nil {
			return err
		}
	}
	return nil
}
