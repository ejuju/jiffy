package jiffy

import "bytes"

func (f *File) Length() int64 { return f.fsize }

func (f *File) Path() string { return f.fpath }

func (f *File) Count() int { return f.midx.count }

// Cursor represents a pointer to a specific key within the linefile.
type Cursor struct {
	f       *File
	current *keyInfo
}

// Seek looks up a key in the memindex.
// If the key is not found, a nil value is returned.
func (f *File) Seek(key []byte) *Cursor {
	if kinfo := f.midx.get(key); kinfo != nil {
		return &Cursor{f: f, current: kinfo}
	}
	return nil
}

// Oldest returns a cursor pointing to the least recently put key in the linefile.
// If the linefile is empty, a nil value is returned.
func (f *File) Oldest() *Cursor {
	if kinfo := f.midx.oldest; kinfo != nil {
		return &Cursor{f: f, current: kinfo}
	}
	return nil
}

// Latest returns the most recently put key in the database.
// If the linefile is empty, a nil value is returned.
func (f *File) Latest() *Cursor {
	if kinfo := f.midx.latest; kinfo != nil {
		return &Cursor{f: f, current: kinfo}
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
	versions []Position
}

// History returns the history associated with the current key that the cursor is pointing to.
func (c *Cursor) History() *History { return &History{f: c.f, versions: c.current.p} }

// Length returns the number of lines in the history.
func (h *History) Length() int { return len(h.versions) }

// Version returns a value in the history given its index.
func (h *History) Version(i int) ([]byte, error) {
	p := h.versions[i]
	buf := make([]byte, p.Length())
	_, err := h.f.r.ReadAt(buf, p.Offset())
	if err != nil {
		return nil, err
	}
	l := Line{}
	_, err = l.ReadFrom(bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	return l.Value, nil
}

// Value returns the last value in the history.
func (h *History) Value() ([]byte, error) { return h.Version(h.Length() - 1) }
