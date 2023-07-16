package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/pkg/kvdb"
)

func main() {
	db, err := kvdb.NewDB("test.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	const numRows = 1_000
	start := time.Now()
	for i := 0; i < numRows; i++ {
		id := make([]byte, 10)
		_, err := rand.Read(id)
		if err != nil {
			panic(err)
		}
		s := []byte(hex.EncodeToString(id))
		err = db.Put(s, s)
		if err != nil {
			panic(err)
		}
	}

	fmt.Printf("write %d rows in %d ms (", numRows, time.Since(start).Milliseconds())
	fmt.Printf("%.4f ops/sec)\n", float64(numRows)/time.Since(start).Seconds())
}
