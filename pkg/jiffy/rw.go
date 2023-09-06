package jiffy

import (
	"bufio"
	"bytes"
	"fmt"
	"time"
)

type Reader struct{ f *File }

func (f *File) Read(do func(r *Reader) error) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	r := &Reader{f: f}
	return do(r)
}

func (r *Reader) Length() int64 { return r.f.fsize }

func (r *Reader) Path() string { return r.f.fpath }

type GroupReader struct {
	f    *File
	gid  GroupID
	midx *memindex
}

func (r *Reader) In(gid GroupID) *GroupReader {
	gmemidx := r.f.memidxs[gid]
	if gmemidx == nil {
		return nil
	}
	return &GroupReader{f: r.f, gid: gid, midx: gmemidx}
}

func (c *GroupReader) Count() int { return c.midx.count }

// Cursor represents a pointer to a specific key within the linefile.
type Cursor struct {
	f       *File
	midx    *memindex
	current *keyInfo
}

// Seek looks up a key in the memindex.
// If the key is not found, a nil value is returned.
func (g *GroupReader) Seek(key []byte) *Cursor {
	if kinfo := g.midx.get(key); kinfo != nil {
		return &Cursor{f: g.f, midx: g.midx, current: kinfo}
	}
	return nil
}

// Oldest returns a cursor pointing to the least recently put key in the linefile.
// If the linefile is empty, a nil value is returned.
func (g *GroupReader) Oldest() *Cursor {
	if kinfo := g.midx.oldest; kinfo != nil {
		return &Cursor{f: g.f, midx: g.midx, current: kinfo}
	}
	return nil
}

// Latest returns the most recently put key in the database.
// If the linefile is empty, a nil value is returned.
func (g *GroupReader) Latest() *Cursor {
	if kinfo := g.midx.latest; kinfo != nil {
		return &Cursor{f: g.f, midx: g.midx, current: kinfo}
	}
	return nil
}

// Next moves the cursor to the next chronological key in the linefile.
// If this is the last key, a nil value is returned.
func (c *Cursor) Next() *Cursor {
	if kinfo := c.current.next; kinfo != nil {
		c.current = kinfo
		return c
	}
	return nil
}

// Previous moves the cursor to the previous chronological key in the linefile.
// If this is the first key, a nil value is returned.
func (c *Cursor) Previous() *Cursor {
	if kinfo := c.current.previous; kinfo != nil {
		c.current = kinfo
		return c
	}
	return nil
}

// Key returns the current key that the cursor points to.
func (c *Cursor) Key() []byte { return c.current.key }

// History holds information about previous operations associated with a given key.
type History struct {
	f        *File
	versions []keyInfoLine
}

// History returns the history associated with the current key that the cursor is pointing to.
func (c *Cursor) History() *History { return &History{f: c.f, versions: c.current.puts} }

// Length returns the number of lines in the history.
func (h *History) Length() int { return len(h.versions) }

type Version struct {
	f        *File
	At       time.Time
	Position Position
}

func (h *History) Version(i int) *Version {
	if len(h.versions) == 0 || i > len(h.versions)-1 {
		return nil
	}
	version := h.versions[i]
	return &Version{f: h.f, At: version.at, Position: version.p}
}

// Value reads the value for the current version.
func (version *Version) Value() ([]byte, error) {
	buf := make([]byte, version.Position.Length())
	_, err := version.f.r.ReadAt(buf, version.Position.Offset())
	if err != nil {
		return nil, err
	}
	_, l, err := version.f.ffmt.Decode(bufio.NewReader(bytes.NewReader(buf)))
	if err != nil {
		return nil, err
	}
	return l.Value, nil
}

// Value returns the last value in the history.
func (h *History) Value() ([]byte, error) { return h.Version(h.Length() - 1).Value() }

type Writer struct {
	f     *File
	lines []Line
}

func (f *File) ReadWrite(do func(r *Reader, w *Writer) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Execute callback, if the callback returns an error, the transaction is aborted.
	r, w := &Reader{f: f}, &Writer{f: f}
	err := do(r, w)
	if err != nil {
		return fmt.Errorf("exec read-write transaction: %w", err)
	}

	// Encode all lines in a temporary buffer
	buf := []byte{}
	positions := make([]Position, 0, len(w.lines))
	for _, l := range w.lines {
		if f.memidxs[l.GroupID] == nil {
			return fmt.Errorf("group with ID %d not found", l.GroupID)
		}
		bufOffset := len(buf)
		encoded, err := f.ffmt.Encode(l)
		if err != nil {
			return err
		}
		positions = append(positions, NewPosition(int64(bufOffset), int64(len(encoded))))
		buf = append(buf, encoded...)
	}

	// Append commit line to buffer
	commitLine, err := f.ffmt.Encode(Line{Op: OpCommit, At: time.Now(), GroupID: GroupID(OpCommit)})
	if err != nil {
		return err
	}
	buf = append(buf, commitLine...)

	// Write buffer to file
	startOffset := f.fsize
	n, err := f.w.Write(buf)
	f.fsize += int64(n)
	if err != nil {
		if n > 0 {
			f.mustTruncateTailCorruption(startOffset)
		}
		return fmt.Errorf("write transaction buffer: %w", err)
	}

	// Ensure file changes are persisted to disk
	err = f.w.Sync()
	if err != nil {
		return fmt.Errorf("sync: %w", err)
	}

	// Update memstate
	for i, l := range w.lines {
		gmidx := f.memidxs[l.GroupID] // nil check is performed during the buffer encoding
		switch l.Op {
		default:
			panic("unreachable")
		case OpPut:
			gmidx.put(l.Key, l.At, positions[i])
		case OpDelete:
			gmidx.delete(l.Key)
		}
	}

	return nil
}

func (w *Writer) In(gid GroupID) *GroupWriter {
	gmemidx := w.f.memidxs[gid]
	if gmemidx == nil {
		return nil
	}
	return &GroupWriter{w: w, gid: gid, midx: gmemidx}
}

type GroupWriter struct {
	w    *Writer
	gid  GroupID
	midx *memindex
}

func (g *GroupWriter) Put(key, value []byte) {
	g.w.lines = append(g.w.lines, Line{Op: OpPut, At: time.Now(), GroupID: g.gid, Key: key, Value: value})
}

func (g *GroupWriter) Delete(key []byte) {
	g.w.lines = append(g.w.lines, Line{Op: OpDelete, At: time.Now(), GroupID: g.gid, Key: key})
}

func (f *File) mustTruncateTailCorruption(truncateAt int64) {
	err := f.w.Truncate(truncateAt)
	if err != nil {
		panic(fmt.Errorf("file tail corruption at offset %d: %w", truncateAt, err))
	}
	f.fsize = truncateAt
	f.initMemstate()
}
