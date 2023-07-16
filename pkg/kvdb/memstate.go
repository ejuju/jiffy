package kvdb

import (
	"fmt"
	"unicode"
)

// Memstate is the in-memory representation of the data state:
// an ordered "map" of keys and their corresponding value on file.
// Here, the memstate is implemented using a trie.
type memstate struct {
	root  *trieNode
	count int // derived state
}

func newMemstate() *memstate { return &memstate{root: &trieNode{}} }

// In-memory data about a row on file.
type keyref struct {
	index int
	width int
}

const (
	// Allow all ASCII characters
	trieMinChar            = uint8(' ')       // included (AASCI whitespace = 32)
	trieMaxChar            = unicode.MaxASCII // included (AASCI delete)
	trieNumChildrenPerNode = trieMaxChar - trieMinChar
)

func trieCharToIndex(c byte) int {
	if c < trieMinChar {
		panic(fmt.Errorf("invalid character: %x", c))
	}
	return int(c) - int(trieMinChar)
}

func trieIndexToChar(i int) byte { return byte(i) + trieMinChar }

type trieNode struct {
	children [trieNumChildrenPerNode]*trieNode
	ref      *keyref // leaf value
}

func (t *memstate) set(key []byte, ref *keyref) {
	n := t.root
	for _, c := range key {
		i := trieCharToIndex(c)
		if n.children[i] == nil {
			n.children[i] = &trieNode{} // create new children if does not exist yet
		}
		n = n.children[i]
	}
	n.ref = ref // set ref on leaf node
	t.count++
}

func (t *memstate) get(key []byte) *keyref {
	n := t.findNode(key)
	if n == nil {
		return nil
	}
	return n.ref // return ref of leaf node (may be nil)
}

func (t *memstate) delete(key []byte) {
	n := t.findNode(key)
	if n == nil {
		return // no-op if key not found
	}
	n.ref = nil // remove ref on node
	t.count--
}

func (t *memstate) scanPrefixWhile(prefix []byte, callback func(k []byte, ref *keyref) bool) {
	n := t.findNode(prefix)
	if n == nil {
		return // no-op if prefix not found
	}
	n.walkRecurse(prefix, callback)
}

func (n *trieNode) walkRecurse(key []byte, callback func(k []byte, ref *keyref) bool) {
	if n.ref != nil {
		ok := callback(key, n.ref)
		if !ok {
			return
		}
	}
	for i, child := range n.children {
		if child == nil {
			continue
		}
		c := trieIndexToChar(i)
		child.walkRecurse(append(key, c), callback)
	}
}

// findNode returns the node corresponding to the last character of the key,
// or nil if the key is not found.
func (t *memstate) findNode(key []byte) *trieNode {
	n := t.root
	for _, c := range key {
		i := trieCharToIndex(c)
		if n.children[i] == nil {
			return nil // return nil if not found
		}
		n = n.children[i]
	}
	return n // return last node
}
