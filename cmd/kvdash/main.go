// Command kvdash serves a small web dashboard for a cluster: who is leader,
// each node's term and log indices, buttons to isolate (partition) or crash
// nodes, and a form to read and write keys.
//
//	kvdash -listen :8080 -nodes 1=http://127.0.0.1:8001,2=http://127.0.0.1:8002,3=http://127.0.0.1:8003
package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed index.html
var indexHTML []byte

type nodeRef struct {
	ID  int
	URL string
}

type nodeView struct {
	ID     int             `json:"id"`
	URL    string          `json:"url"`
	Up     bool            `json:"up"`
	Status json.RawMessage `json:"status,omitempty"`
}

func main() {
	listen := flag.String("listen", ":8080", "HTTP listen address")
	nodesFlag := flag.String("nodes", "1=http://127.0.0.1:8001,2=http://127.0.0.1:8002,3=http://127.0.0.1:8003", "id=http-base-url,...")
	flag.Parse()

	var nodes []nodeRef
	for _, part := range strings.Split(*nodesFlag, ",") {
		id, u, ok := strings.Cut(strings.TrimSpace(part), "=")
		n, err := strconv.Atoi(id)
		if !ok || err != nil {
			log.Fatalf("bad -nodes entry %q", part)
		}
		nodes = append(nodes, nodeRef{ID: n, URL: strings.TrimRight(u, "/")})
	}
	hc := &http.Client{Timeout: 800 * time.Millisecond}
	slow := &http.Client{Timeout: 6 * time.Second}

	find := func(id string) (nodeRef, bool) {
		n, _ := strconv.Atoi(id)
		for _, nd := range nodes {
			if nd.ID == n {
				return nd, true
			}
		}
		return nodeRef{}, false
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		views := make([]nodeView, len(nodes))
		var wg sync.WaitGroup
		for i, nd := range nodes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				views[i] = nodeView{ID: nd.ID, URL: nd.URL}
				resp, err := hc.Get(nd.URL + "/status")
				if err != nil {
					return
				}
				defer resp.Body.Close()
				b, err := io.ReadAll(resp.Body)
				if err == nil && resp.StatusCode == 200 {
					views[i].Up, views[i].Status = true, b
				}
			}()
		}
		wg.Wait()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(views)
	})
	mux.HandleFunc("POST /api/nodes/{id}/{action}", func(w http.ResponseWriter, r *http.Request) {
		nd, ok := find(r.PathValue("id"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		var target string
		switch r.PathValue("action") {
		case "isolate":
			target = "/admin/isolate?on=1"
		case "reconnect":
			target = "/admin/isolate?on=0"
		case "crash":
			target = "/admin/crash"
		default:
			http.NotFound(w, r)
			return
		}
		resp, err := hc.Post(nd.URL+target, "application/json", nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		resp.Body.Close()
		w.WriteHeader(http.StatusNoContent)
	})
	// KV requests go to the first node that answers (each node's /kv uses a
	// cluster client, so it reaches the leader wherever it is).
	mux.HandleFunc("/api/kv/{key...}", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		var lastErr string
		for _, nd := range nodes {
			req, _ := http.NewRequest(r.Method, nd.URL+"/kv/"+r.PathValue("key"), bytes.NewReader(body))
			resp, err := slow.Do(req)
			if err != nil {
				lastErr = err.Error()
				continue
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 500 {
				lastErr = fmt.Sprintf("node %d: %s", nd.ID, strings.TrimSpace(string(b)))
				continue
			}
			w.Header().Set("X-Served-By", strconv.Itoa(nd.ID))
			w.WriteHeader(resp.StatusCode)
			w.Write(b)
			return
		}
		http.Error(w, "no node could serve the request: "+lastErr, http.StatusServiceUnavailable)
	})
	log.Printf("dashboard on %s", *listen)
	log.Fatal(http.ListenAndServe(*listen, mux))
}
