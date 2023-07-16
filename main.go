package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pkg/kvdb"
)

func main() {
	start := time.Now()

	db, err := kvdb.NewDB("test.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	fmt.Printf("ready: loaded %d rows in %.2f seconds\n", db.Count(), time.Since(start).Seconds())

	bufs := bufio.NewScanner(os.Stdin)
	for bufs.Scan() {
		line := bufs.Text()
		parts := strings.Split(line, " ")

		command := parts[0]
		args := parts[1:]
		switch command {
		default:
			fmt.Println("unknown command: ", line)
		case "put":
			err := db.Put([]byte(args[0]), []byte(args[1]))
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("done")
		case "delete":
			err := db.Delete([]byte(args[0]))
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("done")
		case "get":
			v, err := db.Get([]byte(args[0]))
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("%q\n", v)
		case "has":
			if err := checkArgs(args, "key"); err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println(db.Has([]byte(args[0])))
		case "count":
			fmt.Println(db.Count())
		case "compact":
			err := db.Compact()
			if err != nil {
				fmt.Println(db)
				continue
			}
			fmt.Println("done")
		case "scan":
			var err error
			db.ForEachWithPrefix([]byte{}, func(k []byte) bool {
				var v []byte
				v, err = db.Get(k)
				if err != nil {
					return false
				}
				fmt.Printf("%q=%q\n", k, v)
				return true
			})
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "scan-prefix":
			var err error
			db.ForEachWithPrefix([]byte(args[0]), func(k []byte) bool {
				var v []byte
				v, err = db.Get(k)
				if err != nil {
					return false
				}
				fmt.Printf("%q=%q\n", k, v)
				return true
			})
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "head":
			var err error
			max := 10
			i := 0
			db.ForEachWithPrefix([]byte{}, func(k []byte) bool {
				var v []byte
				v, err = db.Get(k)
				if err != nil {
					return false
				}
				if i > max {
					return false
				}
				i++
				fmt.Printf("%s=%s\n", k, v)
				return true
			})
			if err != nil {
				fmt.Println(err)
				continue
			}
		}
	}
	if err := bufs.Err(); err != nil {
		panic(err)
	}
}

func checkArgs(args []string, labels ...string) error {
	if len(args) < len(labels) {
		return fmt.Errorf("want %d arguments (%s) but got %d", len(labels), labels, len(args))
	}
	return nil
}
