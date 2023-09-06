package jiffy

import (
	"bytes"
	"time"
)

type memindex struct {
	count          int        // number of unique non-deleted keys
	oldest, latest *keyInfo   // links to oldest and latest items in chronological order
	buckets        []*keyInfo // number of hashtable buckets (for separate chaining)
}

type keyInfo struct {
	key            []byte
	puts           []keyInfoLine
	previous, next *keyInfo
	nextInBucket   *keyInfo // internal hashtable bucket state for seperate chaining
}

type keyInfoLine struct {
	p  Position
	at time.Time
}

type Position [2]int64

func NewPosition(offset, size int64) Position { return Position{offset, size} }
func (p Position) Offset() int64              { return p[0] }
func (p Position) Length() int64              { return p[1] }

func newMemindex(numBuckets int) *memindex {
	if numBuckets == 0 {
		numBuckets = 1
	}
	return &memindex{buckets: make([]*keyInfo, numBuckets)}
}

func (lht *memindex) put(key []byte, at time.Time, p Position) {
	bucketIndex := lht.hashFNV1a(key)
	root := lht.buckets[bucketIndex]
	var prevInBucket *keyInfo
	for item := root; item != nil; prevInBucket, item = item, item.nextInBucket {
		if bytes.Equal(item.key, key) {
			// Append put position and move exisiting item to end of linked-list
			item.puts = append(item.puts, keyInfoLine{at: at, p: p})
			if item != lht.latest {
				if item == lht.oldest {
					lht.oldest = item.next
				}
				item.next.previous, item.next = item.previous, nil
				item.previous, lht.latest.next = lht.latest, item // link to previous item
				lht.latest = item                                 // set latest to current item
			}
			return
		}
	}

	// Add new item to bucket and increment count
	lht.count++
	newItem := &keyInfo{key: key, puts: []keyInfoLine{{at: at, p: p}}}
	if prevInBucket == nil {
		lht.buckets[bucketIndex] = newItem
	} else {
		prevInBucket.nextInBucket = newItem
	}

	// Add at the end of linked-list
	if lht.latest == nil {
		lht.oldest, lht.latest = newItem, newItem
	} else {
		newItem.previous, lht.latest.next = lht.latest, newItem // link to previous item
		lht.latest = newItem                                    // set latest to new item
	}
}

func (lht *memindex) delete(key []byte) {
	bucketIndex := lht.hashFNV1a(key)
	root := lht.buckets[bucketIndex]
	var prevInBucket *keyInfo
	for item := root; item != nil; prevInBucket, item = item, item.nextInBucket {
		if bytes.Equal(item.key, key) {
			// Remove from bucket and decrement count
			lht.count--
			if prevInBucket != nil {
				prevInBucket.nextInBucket = item.nextInBucket
			}

			// Unlink from linked-list
			if item.previous == nil {
				lht.oldest = item.next
			} else {
				item.previous.next = item.next
			}
			if item.next == nil {
				lht.latest = item.previous
			} else {
				item.next.previous = item.previous
			}
			return
		}
	}
}

func (lht *memindex) get(key []byte) *keyInfo {
	root := lht.buckets[lht.hashFNV1a(key)]
	for item := root; item != nil; item = item.nextInBucket {
		if bytes.Equal(item.key, key) {
			return item
		}
	}
	return nil
}

func (lht *memindex) hashFNV1a(key []byte) int {
	const offset, prime = uint64(14695981039346656037), uint64(1099511628211) // fnv-1a constants
	hash := offset
	for _, char := range key {
		hash *= prime
		hash ^= uint64(char)
	}
	index := int(hash % uint64(len(lht.buckets)))
	return index
}
