// Command kvcli is a command-line client for the cluster.
//
//	kvcli -servers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 put x 5
//	kvcli -servers ... get x
//	kvcli -servers ... delete x
//	kvcli -servers ... status
//	kvcli -servers ... members add 4 127.0.0.1:7004
//	kvcli -servers ... members remove 4
//	kvcli -servers ... bench 1000
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/anair0/distributed-key-value-store-with-raft/internal/node"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/raft"
	"github.com/anair0/distributed-key-value-store-with-raft/internal/transport/tcp"
)

func main() {
	servers := flag.String("servers", "1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003", "cluster members: id=host:port,...")
	timeout := flag.Duration("timeout", 10*time.Second, "overall timeout")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: kvcli [-servers ...] get KEY | put KEY VALUE | delete KEY | status | members add ID ADDR | members remove ID | bench N")
		flag.PrintDefaults()
	}
	flag.Parse()
	members, err := node.ParseMembers(*servers)
	if err != nil || len(members) == 0 || flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	pool := tcp.NewPool(nil)
	defer pool.Close()
	cl := tcp.NewClient(pool, members)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	args := flag.Args()
	need := func(n int) {
		if len(args) != n {
			flag.Usage()
			os.Exit(2)
		}
	}
	switch args[0] {
	case "get":
		need(2)
		v, ok, err := cl.Get(ctx, args[1])
		check(err)
		if !ok {
			fmt.Fprintln(os.Stderr, "(not found)")
			os.Exit(1)
		}
		fmt.Println(string(v))
	case "put", "set":
		need(3)
		check(cl.Put(ctx, args[1], []byte(args[2])))
		fmt.Println("OK")
	case "delete", "del":
		need(2)
		existed, err := cl.Delete(ctx, args[1])
		check(err)
		fmt.Println("OK, existed:", existed)
	case "status":
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tADDR\tROLE\tTERM\tLEADER\tCOMMIT\tAPPLIED\tLAST LOG\tSNAPSHOT\tMEMBERS")
		for _, m := range members {
			sctx, c := context.WithTimeout(ctx, time.Second)
			st, err := tcp.Status(sctx, pool, m.Addr)
			c()
			if err != nil {
				fmt.Fprintf(w, "%d\t%s\tDOWN\t\t\t\t\t\t\t\n", m.ID, m.Addr)
				continue
			}
			fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n", m.ID, m.Addr, st.Role, st.Term, st.LeaderID, st.CommitIndex, st.LastApplied, st.LastLogIndex, st.SnapshotIndex, len(st.Members))
		}
		w.Flush()
	case "members":
		if len(args) < 3 {
			flag.Usage()
			os.Exit(2)
		}
		id, err := strconv.Atoi(args[2])
		check(err)
		switch args[1] {
		case "add":
			need(4)
			check(cl.AddMember(ctx, raft.Member{ID: raft.NodeID(id), Addr: args[3]}))
		case "remove":
			need(3)
			check(cl.RemoveMember(ctx, raft.NodeID(id)))
		default:
			flag.Usage()
			os.Exit(2)
		}
		fmt.Println("OK")
	case "bench":
		need(2)
		n, err := strconv.Atoi(args[1])
		check(err)
		start := time.Now()
		for i := range n {
			check(cl.Put(ctx, fmt.Sprintf("bench-%d", i), []byte("x")))
		}
		d := time.Since(start)
		fmt.Printf("%d sequential puts in %v (%.0f ops/s, %v/op)\n", n, d.Round(time.Millisecond), float64(n)/d.Seconds(), (d / time.Duration(n)).Round(time.Microsecond))
	default:
		flag.Usage()
		os.Exit(2)
	}
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
