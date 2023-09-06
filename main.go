package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ejuju/jiffy/pkg/jiffy"
)

func main() {
	panic("todo: implement TCP server")

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	if len(os.Args) <= 1 {
		fmt.Println("missing database file path")
		return
	}

	start := time.Now()
	path := os.Args[1]
	// mode := os.Args[2] // 'server' or 'repl'

	f, err := jiffy.Open(path, nil, map[jiffy.GroupID]int{0: 0})
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	fmt.Printf("Loaded %q in %s\nType a command and press enter: ", path, time.Since(start))

	go func() {
		bufs := bufio.NewScanner(os.Stdin)
		for bufs.Scan() {
			handleCommand(f, bufs.Text())
			fmt.Print("\n? ")
		}
		if err := bufs.Err(); err != nil {
			panic(err)
		}
	}()

	<-interrupt
	err = f.Close()
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("goodbye!")
}
