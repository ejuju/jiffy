# LilDB: Tiny embedded key-value database for Go applications

## Supported operations:
- [x] Create or update a key-value pair in the database: `db.Put(key, value)`
- [x] Delete a key-value pair: `db.Delete(key)`
- [x] Get the value for a given key: `db.Get(key)`
- [x] Iterate over keys in lexicographical order: `db.ForEach(prefix, callback)`


## Characteristics

- Append-only (updates and deletes do not erase previous data).
- Keys are ordered lexicographically in a trie (time complexity O(N) where N is key-length).
- A "get" operation maps to a single OS file read.
- A "put" or "delete" operation maps to a single OS file write.

## File format

A data file consists of consecutive rows.

Here's what a row looks like:

```
+------------+--------------------+-----------------------+---------+-----------+----+
| op (uint8) | key-length (uint8) | value-length (uint32) | key ... | value ... | LF |
+------------+--------------------+-----------------------+---------+-----------+----+
```

Notes:
- Timestamp (big-endian uint64) represents the number of seconds since Unix epoch.
- Op (single byte character) represents a "put" or "delete" operation.
- Key-length (uint8) represents the width of the key.
- Value-length (big-endian uint32) represents the width of the value.
