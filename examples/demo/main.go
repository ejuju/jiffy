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

	// Put a key-value pair in the database.
	err = f.ReadWrite(func(r *jiffy.Reader, w *jiffy.Writer) error {
		return w.Into(userGroupID).Put(key1, value1)
	})
	if err != nil {
		panic(err)
	}

	// Delete a key-value pair from the database.
	err = f.ReadWrite(func(r *jiffy.Reader, w *jiffy.Writer) error {
		return w.Into(userGroupID).Delete([]byte("006"))
	})
	if err != nil {
		panic(err)
	}

	// Get a key-value pair
	f.Read(func(r *jiffy.Reader) error {
		c := r.Inside(userGroupID).Seek(key1)
		if c == nil {
			log.Println("key not found")
		}
		v, err := c.History().Value()
		if err != nil {
			panic(err)
		}
		log.Println(v)
		return nil
	})

	// Iterate over a given key's history of values
	f.Read(func(r *jiffy.Reader) error {
		history := r.Inside(userGroupID).Seek(key1).History()
		for i := 0; i < history.Length(); i++ {
			version := history.Version(i)
			v, err := version.Value()
			if err != nil {
				panic(err)
			}
			log.Println(version.At.Format(time.RFC3339), v)
		}
		return nil
	})

	// Check if a key-value pair exists
	f.Read(func(r *jiffy.Reader) error {
		if r.Inside(userGroupID).Seek(key1) != nil {
			log.Println("already exists")
		}
		return nil
	})

	// Count unique non-delete keys
	f.Read(func(r *jiffy.Reader) error {
		count := r.Inside(userGroupID).Count()
		log.Println(count)
		return nil
	})

	// Iterate over keys in order
	f.Read(func(r *jiffy.Reader) error {
		users := r.Inside(userGroupID)

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
		return nil
	})

}

func init() {
	log.SetFlags(log.Lshortfile)
}
