// Command kvserver runs a single node of the replicated KV store.
package main

import (
	"flag"
	"fmt"
)

func main() {
	id := flag.Int("id", 0, "node ID")
	peers := flag.String("peers", "", "comma-separated peer addresses")
	flag.Parse()
	fmt.Printf("kvserver id=%d peers=%q (not implemented yet)\n", *id, *peers)
}
