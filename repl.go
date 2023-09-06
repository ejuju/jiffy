package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ejuju/jiffy/pkg/jiffy"
)

func handleCommand(f *jiffy.File, line string) {
	parts := strings.Split(line, " ")
	keyword := parts[0]

	// Find and exec command
	for _, cmd := range commands {
		isMatch := false
		for _, kw := range cmd.keywords {
			if kw == keyword {
				isMatch = true
			}
		}
		if isMatch {
			var args []string
			if len(cmd.args) > 0 {
				if len(parts)-1 != len(cmd.args) {
					fmt.Printf("%q needs %d argument(s): %s\n", keyword, len(cmd.args), strings.Join(cmd.args, ", "))
					return
				}
				args = parts[1:]
			}
			cmd.do(f, args...)
			return
		}
	}

	fmt.Printf("\nCommand not found: %q\n", keyword)
	printAvailableCommands(commands)
}

func printAvailableCommands(commands []*command) {
	fmt.Println("Available commands:")
	for _, cmd := range commands {
		fmt.Printf("> \033[033m%-15s\033[0m \033[2m%s\033[0m\n", cmd.keywords[0], cmd.desc)
	}
}

type command struct {
	desc     string
	keywords []string
	args     []string
	do       func(f *jiffy.File, args ...string)
}

var commands = []*command{
	// {
	// 	keywords: []string{"compact"},
	// 	desc:     "removes deleted key-value pairs and re-writes rows in lexicographical order",
	// 	do: func(f *jiffy.File, args ...string) {
	// 		start := time.Now()
	// 		err := f.Compact()
	// 		if err != nil {
	// 			fmt.Println(err)
	// 			return
	// 		}
	// 		fmt.Printf("compacted in %s\n", time.Since(start))
	// 	},
	// },
	{
		keywords: []string{"set", "+"},
		desc:     "set a key-value pair in the database",
		args:     []string{"group ID", "key", "value"},
		do: func(f *jiffy.File, args ...string) {
			gid, key, value := jiffy.GroupID(args[0][0]), []byte(args[1]), []byte(args[2])
			err := f.ReadWrite(func(r *jiffy.Reader, w *jiffy.Writer) error {
				w.In(gid).Put(key, value)
				return nil
			})
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Printf("%q is now %q\n", key, value)
		},
	},
	{
		keywords: []string{"delete", "-"},
		desc:     "delete a key-value pair from the database",
		args:     []string{"group ID", "key"},
		do: func(f *jiffy.File, args ...string) {
			gid, key := jiffy.GroupID(args[0][0]), []byte(args[1])
			err := f.ReadWrite(func(r *jiffy.Reader, w *jiffy.Writer) error {
				w.In(gid).Delete(key)
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
		keywords: []string{"get"},
		desc:     "get the value associated with a given key",
		args:     []string{"group ID", "key"},
		do: func(f *jiffy.File, args ...string) {
			gid, key := jiffy.GroupID(args[0][0]), []byte(args[1])
			_ = f.Read(func(r *jiffy.Reader) error {
				c := r.In(gid).Seek(key)
				if c == nil {
					fmt.Printf("%q not found\n", key)
					return nil
				}
				value, err := c.History().Value()
				if err != nil {
					fmt.Println(err)
					return nil
				}
				fmt.Printf("%q = %q\n", key, value)
				return nil
			})
		},
	},
	{
		keywords: []string{"has", "?"},
		desc:     "reports whether a key exists",
		args:     []string{"group ID", "key"},
		do: func(f *jiffy.File, args ...string) {
			gid, key := jiffy.GroupID(args[0][0]), []byte(args[1])
			_ = f.Read(func(r *jiffy.Reader) error {
				fmt.Println(r.In(gid).Seek(key) != nil)
				return nil
			})
		},
	},
	{
		keywords: []string{"count"},
		desc:     "reports the number of unique keys",
		args:     []string{"group ID"},
		do: func(f *jiffy.File, args ...string) {
			gid := jiffy.GroupID(args[0][0])
			_ = f.Read(func(r *jiffy.Reader) error {
				fmt.Println(r.In(gid).Count())
				return nil
			})
		},
	},
	{
		keywords: []string{"all"},
		desc:     "show all unique keys",
		args:     []string{"group ID"},
		do: func(f *jiffy.File, args ...string) {
			gid := jiffy.GroupID(args[0][0])
			_ = f.Read(func(r *jiffy.Reader) error {
				for rr := r.In(gid).Oldest(); rr != nil; rr = rr.Next() {
					fmt.Printf("%q\n", rr.Key())
				}
				return nil
			})
		},
	},
	{
		keywords: []string{"tail"},
		desc:     "show the last 10 key-value pairs",
		args:     []string{"group ID"},
		do: func(f *jiffy.File, args ...string) {
			gid := jiffy.GroupID(args[0][0])
			_ = f.Read(func(r *jiffy.Reader) error {
				i := 0
				for c := r.In(gid).Latest(); c != nil; c = c.Previous() {
					if i >= 10 {
						break
					}
					i++
					v, err := c.History().Value()
					if err != nil {
						fmt.Println(err)
						return nil
					}
					fmt.Printf("%q = %q\n", c.Key(), v)
				}
				return nil
			})
		},
	},
	{
		keywords: []string{"head"},
		desc:     "show the first 10 key-value pairs",
		args:     []string{"group ID"},
		do: func(f *jiffy.File, args ...string) {
			gid := jiffy.GroupID(args[0][0])
			_ = f.Read(func(r *jiffy.Reader) error {
				i := 0
				for c := r.In(gid).Oldest(); c != nil; c = c.Next() {
					if i >= 10 {
						break
					}
					i++
					v, err := c.History().Value()
					if err != nil {
						fmt.Println(err)
						return nil
					}
					fmt.Printf("%q = %q\n", c.Key(), v)
				}
				return nil
			})
		},
	},
	{
		keywords: []string{"fill"},
		desc:     "fill the database with the given number of key-value pairs",
		args:     []string{"group ID", "number"},
		do: func(f *jiffy.File, args ...string) {
			gid := jiffy.GroupID(args[0][0])
			start := time.Now()
			num, err := strconv.Atoi(args[0])
			if err != nil {
				fmt.Println(err)
				return
			}
			err = f.ReadWrite(func(r *jiffy.Reader, w *jiffy.Writer) error {
				for i := 0; i < num; i++ {
					key := []byte(strconv.Itoa(i))
					value := []byte(time.Now().Format(time.RFC3339))
					w.In(gid).Put(key, value)
				}
				return nil
			})
			if err != nil {
				fmt.Println(err)
				return
			}
			elapsed := time.Since(start)
			fmt.Printf("added %d rows in %s\n", num, elapsed)
		},
	},
}
