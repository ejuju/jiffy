package main

import (
	"log"

	"github.com/ejuju/jiffy/pkg/jiffy"
)

const (
	_ jiffy.GroupID = iota
	userGroupID
	contactGroupID
)

func main() {
	key1 := []byte{007}
	value1 := []byte("James Bond")

	f, err := jiffy.Open("db.lf", map[jiffy.GroupID]int{
		userGroupID:    0,
		contactGroupID: 0,
	})
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Scope operations to a certain group (= "table" or "collection")
	users := f.Inside(userGroupID)

	// Put a key-value pair in the database.
	err = users.Put(key1, value1)
	if err != nil {
		panic(err)
	}

	// Delete a key-value pair from the database.
	err = f.Inside(userGroupID).Delete([]byte{123})
	if err != nil {
		panic(err)
	}

	// Sync the underlying database file
	err = f.Sync()
	if err != nil {
		panic(err)
	}

	// Lookup a key
	c := users.Seek(key1)
	if c == nil {
		log.Println("key not found")
	}
	v, err := c.History().Value()
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
	if users.Seek(key1) != nil {
		log.Println("already exists")
	}

	// Count unique non-delete keys
	count := users.Count()
	log.Println(count)

	// Iterate over keys in chronological order
	for c := users.Oldest(); c != nil; c = c.Next() {
		log.Println(c.Key())
	}

	// Iterate over keys in reverse chronological order
	for c := users.Latest(); c != nil; c = c.Previous() {
		log.Println(c.Key())
	}

	// Iterate over keys created or updated after a given key
	for c := users.Seek(key1); c != nil; c = c.Next() {
		log.Println(c.Key())
	}
}

func init() {
	log.SetFlags(log.Lshortfile)
}
