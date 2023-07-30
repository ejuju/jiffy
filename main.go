package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ejuju/jiffydb/pkg/jiffy"
)

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	if len(os.Args) <= 1 {
		fmt.Println("missing directory path")
		return
	}

	start := time.Now()
	fpath := os.Args[1]
	db := jiffy.New(fpath)
	defer db.Close()

	fmt.Printf("Loaded %q in %s\nType a command and press enter: ", fpath, time.Since(start))

	go func() {
		bufs := bufio.NewScanner(os.Stdin)
		for bufs.Scan() {
			handleCommand(db, bufs.Text())
			fmt.Print("\n? ")
		}
		if err := bufs.Err(); err != nil {
			panic(err)
		}
	}()

	<-interrupt
	errs := db.Close()
	if len(errs) > 0 {
		panic(errs)
	}
}

func handleCommand(db *jiffy.Database, line string) {
	parts := strings.Split(line, " ")
	keyword := parts[0]

	// Find and exec command
	for _, cmd := range commands {
		if cmd.keyword == keyword {
			var args []string
			if len(cmd.args) > 0 {
				if len(parts)-1 != len(cmd.args) {
					fmt.Printf("%q needs %d argument(s): %s\n", keyword, len(cmd.args), strings.Join(cmd.args, ", "))
					return
				}
				args = parts[1:]
			}
			cmd.do(db, args...)
			return
		}
	}

	fmt.Printf("\nCommand not found: %q\nAvailable commands:\n", keyword)
	printAvailableCommands(commands)
}

func printAvailableCommands(commands []*command) {
	for _, cmd := range commands {
		fmt.Printf("> \033[033m%-12s\033[0m \033[2m%s\033[0m\n", cmd.keyword, cmd.desc)
	}
}

type command struct {
	desc    string
	keyword string
	args    []string
	do      func(f *jiffy.Database, args ...string)
}

var commands = []*command{
	{
		keyword: "with",
		desc:    "use a list in the database (and create datafile if needed)",
		args:    []string{"list name"},
		do: func(f *jiffy.Database, args ...string) {
			name := args[0]
			err := f.ReadWrite(func(r *jiffy.Reader, w *jiffy.Writer) error { return w.With(name) })
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Printf("%q opened\n", name)
		},
	},
	{
		keyword: "put",
		desc:    "set a key-value pair in the database",
		args:    []string{"list", "key", "value"},
		do: func(f *jiffy.Database, args ...string) {
			list, key, value := args[0], []byte(args[1]), []byte(args[2])
			err := f.ReadWrite(func(r *jiffy.Reader, w *jiffy.Writer) error {
				w.In(list).Put(key, value)
				return nil
			})
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Printf("%q = %q\n", key, value)
		},
	},
	{
		keyword: "delete",
		desc:    "delete a key-value pair from the database",
		args:    []string{"list", "key"},
		do: func(db *jiffy.Database, args ...string) {
			list, key := args[0], []byte(args[1])
			err := db.ReadWrite(func(r *jiffy.Reader, w *jiffy.Writer) error {
				w.In(list).Delete(key)
				return nil
			})
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Printf("deleted %q\n", key)
		},
	},
	{
		keyword: "get",
		desc:    "get a key-value pair from the database",
		args:    []string{"list", "key"},
		do: func(db *jiffy.Database, args ...string) {
			list, key := args[0], []byte(args[1])
			var value []byte
			err := db.Read(func(r *jiffy.Reader) error {
				v, err := r.In(list).Get(key)
				value = v
				return err
			})
			if err != nil {
				fmt.Println(err)
				return
			}
			if value == nil {
				fmt.Printf("%q not found\n", key)
				return
			}
			fmt.Printf("%q = %q\n", key, value)
		},
	},
	{
		keyword: "has",
		desc:    "reports whether a key exists in a list",
		args:    []string{"list", "key"},
		do: func(db *jiffy.Database, args ...string) {
			list, key := args[0], []byte(args[1])
			_ = db.Read(func(r *jiffy.Reader) error {
				fmt.Println(r.In(list).Exists(key))
				return nil
			})
		},
	},
	{
		keyword: "countlist",
		desc:    "counts the number of lists in the database",
		do: func(db *jiffy.Database, args ...string) {
			_ = db.Read(func(r *jiffy.Reader) error {
				fmt.Println(r.NumberOfLists())
				return nil
			})
		},
	},
	{
		keyword: "all",
		args:    []string{"list"},
		desc:    "show all key-value pairs in a given list",
		do: func(db *jiffy.Database, args ...string) {
			list := args[0]
			_ = db.Read(func(r *jiffy.Reader) error {
				return r.In(list).Walk([]byte{}, func(key []byte) (bool, error) {
					fmt.Printf("Found %q\n", key)
					return true, nil
				})
			})
		},
	},
	{
		keyword: "fill",
		desc:    "fill the database with the given number of key-value pairs",
		args:    []string{"list", "number"},
		do: func(f *jiffy.Database, args ...string) {
			start := time.Now()
			num, err := strconv.Atoi(args[1])
			if err != nil {
				fmt.Println(err)
				return
			}
			for i := 0; i < num; i++ {
				list, key := args[0], []byte(strconv.Itoa(i))
				err := f.ReadWrite(func(r *jiffy.Reader, w *jiffy.Writer) error {
					w.In(list).Put(key, key)
					return nil
				})
				if err != nil {
					fmt.Println(err)
					return
				}
			}
			fmt.Printf("added row 0 to %d in %s\n", num, time.Since(start))
		},
	},
}
