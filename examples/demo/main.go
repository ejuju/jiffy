package main

import (
	"fmt"

	"github.com/ejuju/jiffy/pkg/jiffy"
)

const (
	todoList = "todo"
	userList = "user"
)

func main() {
	// Instanciate the database and open list files (=~ tables)
	db := jiffy.New(".db")
	defer db.Close()
	err := db.ReadWrite(func(r *jiffy.Reader, w *jiffy.Writer) error {
		return w.With(todoList, userList)
	})
	if err != nil {
		panic(err)
	}

	// Set a key-value pair
	err = db.ReadWrite(func(r *jiffy.Reader, w *jiffy.Writer) error {
		w.In(todoList).Put([]byte("laundry"), []byte("todo"))
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Set multiple key-value pairs
	err = db.ReadWrite(func(r *jiffy.Reader, w *jiffy.Writer) error {
		w.In(todoList).Put([]byte("vacuum room"), []byte("todo"))
		w.In(todoList).Put([]byte("groceries"), []byte("todo"))
		w.In(todoList).Put([]byte("laundry"), []byte("done"))
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Delete a key-value pair
	err = db.ReadWrite(func(r *jiffy.Reader, w *jiffy.Writer) error {
		w.In(todoList).Delete([]byte("vacuum room"))
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Get a key-value pair
	// Note: key not found returns a nil value and no error.
	var v []byte
	err = db.Read(func(r *jiffy.Reader) error {
		v, err = r.In(todoList).Get([]byte("laundry"))
		return err
	})
	if err != nil {
		panic(err)
	}
	if v == nil {
		fmt.Println("not found")
	}
	fmt.Printf("%q\n", v)

	// Check if a key exists
	_ = db.Read(func(r *jiffy.Reader) error {
		exists := r.In(todoList).Exists([]byte("laundry"))
		fmt.Println(exists)
		return nil
	})

	// Walk all keys
	_ = db.Read(func(r *jiffy.Reader) error {
		return r.In(todoList).Walk([]byte{}, func(key []byte) (bool, error) {
			fmt.Printf("%q\n", key)
			return true, nil
		})
	})

	// Walk all key-value pairs
	_ = db.Read(func(r *jiffy.Reader) error {
		return r.In(todoList).WalkWithValue([]byte{}, func(key, value []byte) (bool, error) {
			fmt.Printf("%q = %q\n", key, value)
			return true, nil
		})
	})

	// Walk all keys with prefix
	_ = db.Read(func(r *jiffy.Reader) error {
		return r.In(todoList).Walk([]byte("laun"), func(key []byte) (bool, error) {
			fmt.Printf("%q\n", key)
			return true, nil
		})
	})

	// Walk some keys (return false in callback)
	count := 10
	_ = db.Read(func(r *jiffy.Reader) error {
		return r.In(todoList).Walk([]byte("2006"), func(key []byte) (bool, error) {
			if count == 0 {
				return false, nil
			}
			fmt.Printf("%q\n", key)
			count--
			return true, nil
		})
	})
}
