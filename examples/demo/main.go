package main

import (
	"log"

	"github.com/ejuju/jiffy/pkg/jiffy"
)

func main() {
	f, err := jiffy.Open("main.lf", 0)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Put a key-value pair in the database
	err = f.Put([]byte{123}, []byte("something"))
	if err != nil {
		panic(err)
	}

	// Delete a key-value pair in the database
	err = f.Delete([]byte{123})
	if err != nil {
		panic(err)
	}

	// Sync the underlying database file
	err = f.Sync()
	if err != nil {
		panic(err)
	}

	// Lookup a key
	c := f.Seek([]byte{1})
	if c == nil {
		log.Println("key not found")
	}
	v, err := f.Seek([]byte{1}).History().Value()
	if err != nil {
		panic(err)
	}
	log.Println(v)

	// Iterate over a given key's history of values
	khistory := c.History()
	for i := 0; i < khistory.Length(); i++ {
		v, err = khistory.Version(i)
		if err != nil {
			panic(err)
		}
		log.Println(v)
	}

	// Check if a key-value pair exists
	if f.Seek([]byte{1}) != nil {
		log.Println("already exists")
	}

	// Count unique non-delete keys
	count := f.Count()
	log.Println(count)

	// Iterate over keys in chronological order
	for c := f.Oldest(); c != nil; c = c.Next() {
		log.Println(c.Key())
	}

	// Iterate over keys in reverse chronological order
	for c := f.Latest(); c != nil; c = c.Previous() {
		log.Println(c.Key())
	}

	// Iterate over keys put after a given key
	for c := f.Seek([]byte{123}); c != nil; c = c.Next() {
		log.Println(c.Key())
	}
}
