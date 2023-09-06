package main

import (
	"log"
	"time"

	"github.com/ejuju/jiffy/pkg/jiffy"
)

const (
	userGroupID    = jiffy.GroupID('u')
	contactGroupID = jiffy.GroupID('c')
)

func main() {
	key1 := []byte("007")
	value1 := []byte("James Bond")

	f, err := jiffy.Open("db.lf", nil, map[jiffy.GroupID]int{
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
	err = users.Delete([]byte("006"))
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
		version := khistory.Version(i)
		v, err := version.Value()
		if err != nil {
			panic(err)
		}
		log.Println(version.At.Format(time.RFC3339), v)
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
