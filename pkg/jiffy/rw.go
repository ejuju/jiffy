package jiffy

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"
)

func (f *File) Length() int64 { return f.fsize }

func (f *File) Path() string { return f.fpath }

func (f *File) Sync() error { return f.w.Sync() }

type Group struct {
	f    *File
	gid  GroupID
	midx *memindex
}

func (f *File) Inside(gid GroupID) *Group {
	gmemidx := f.memidxs[gid]
	if gmemidx == nil {
		return nil
	}
	return &Group{f: f, gid: gid, midx: gmemidx}
}

func (g *Group) Put(key, value []byte) error {
	offset, length, err := g.f.append(OpPut, g.gid, time.Now(), key, value)
	if err != nil {
		return err
	}
	g.midx.put(key, time.Now(), NewPosition(offset, length))
	return nil
}

func (g *Group) Delete(key []byte) error {
	_, _, err := g.f.append(OpDelete, g.gid, time.Now(), key, nil)
	if err != nil {
		return err
	}
	g.midx.delete(key)
	return nil
}

func (f *File) append(opcode Opcode, gid GroupID, at time.Time, slot1, slot2 []byte) (int64, int64, error) {
	startOffset := f.fsize
	encoded, err := f.ffmt.Encode(Line{Op: opcode, GroupID: gid, At: at, Key: slot1, Value: slot2})
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

func (c *Group) Count() int { return c.midx.count }

// Cursor represents a pointer to a specific key within the linefile.
type Cursor struct {
	f       *File
	midx    *memindex
	current *keyInfo
}

// Seek looks up a key in the memindex.
// If the key is not found, a nil value is returned.
func (g *Group) Seek(key []byte) *Cursor {
	if kinfo := g.midx.get(key); kinfo != nil {
		return &Cursor{f: g.f, midx: g.midx, current: kinfo}
	}
	return nil
}

// Oldest returns a cursor pointing to the least recently put key in the linefile.
// If the linefile is empty, a nil value is returned.
func (g *Group) Oldest() *Cursor {
	if kinfo := g.midx.oldest; kinfo != nil {
		return &Cursor{f: g.f, midx: g.midx, current: kinfo}
	}
	return nil
}

// Latest returns the most recently put key in the database.
// If the linefile is empty, a nil value is returned.
func (g *Group) Latest() *Cursor {
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
