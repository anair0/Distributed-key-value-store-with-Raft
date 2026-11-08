package kv

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/anair0/distributed-key-value-store-with-raft/internal/raft"
)

// Conn sends requests to one server (over the fake network or TCP).
type Conn interface {
	Do(ctx context.Context, req *Request) (*Response, error)
}

// Peer is one server the client can talk to.
type Peer struct {
	ID   raft.NodeID
	Conn Conn
}

// Client is the client library ("clerk"). It finds the leader, retries on
// failure, and numbers requests so retries are applied at most once.
// A Client issues one request at a time; use one Client per goroutine.
type Client struct {
	mu       sync.Mutex
	peers    []Peer
	clientID string
	seq      uint64
	leader   int

	// AttemptTimeout bounds one attempt against one server.
	AttemptTimeout time.Duration
	// Dial, if set, is used to reach a leader hinted by address that is not
	// in peers (e.g. a node added by a membership change).
	Dial func(id raft.NodeID, addr string) Conn
}

// NewClient creates a client for the given servers.
func NewClient(peers []Peer) *Client {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return &Client{
		peers:          peers,
		clientID:       hex.EncodeToString(b[:]),
		AttemptTimeout: 3 * time.Second,
	}
}

// ID returns the client's unique ID.
func (c *Client) ID() string { return c.clientID }

// Get returns the value of key and whether it exists.
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	resp, err := c.Do(ctx, &Request{Op: OpGet, Key: key})
	if err != nil {
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

// Put sets key to value.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	_, err := c.Do(ctx, &Request{Op: OpPut, Key: key, Value: value})
	return err
}

// Delete removes key and reports whether it existed.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	resp, err := c.Do(ctx, &Request{Op: OpDelete, Key: key})
	if err != nil {
		return false, err
	}
	return resp.Found, nil
}

// AddMember asks the cluster to add a node.
func (c *Client) AddMember(ctx context.Context, m raft.Member) error {
	_, err := c.Do(ctx, &Request{Op: OpAddMember, Member: m})
	return err
}

// RemoveMember asks the cluster to remove a node.
func (c *Client) RemoveMember(ctx context.Context, id raft.NodeID) error {
	_, err := c.Do(ctx, &Request{Op: OpRemoveMember, Member: raft.Member{ID: id}})
	return err
}

// ErrBadKey is returned for keys reserved for internal use.
var ErrBadKey = errors.New("kv: keys may not start with \\x00")

// Do sends req until it succeeds or ctx is done. The same sequence number is
// used for every retry of the request.
func (c *Client) Do(ctx context.Context, req *Request) (*Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	r := *req
	r.ClientID, r.Seq = c.clientID, c.seq

	tried := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p := c.peers[c.leader]
		actx, cancel := context.WithTimeout(ctx, c.AttemptTimeout)
		resp, err := p.Conn.Do(actx, &r)
		cancel()
		followed := false
		if err == nil {
			switch resp.Err {
			case OK:
				return resp, nil
			case ErrBadRequest:
				return nil, ErrBadKey
			case ErrWrongLeader:
				followed = c.follow(resp.LeaderID, resp.LeaderAddr)
			}
		}
		if !followed {
			c.leader = (c.leader + 1) % len(c.peers)
		}
		tried++
		if tried%len(c.peers) == 0 {
			// A full round without success (e.g. an election is running).
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
}

// follow switches to the hinted leader. It reports whether it switched.
func (c *Client) follow(id raft.NodeID, addr string) bool {
	if id == raft.None {
		return false
	}
	for i, p := range c.peers {
		if p.ID == id {
			if i == c.leader {
				return false
			}
			c.leader = i
			return true
		}
	}
	if c.Dial != nil && addr != "" {
		c.peers = append(c.peers, Peer{ID: id, Conn: c.Dial(id, addr)})
		c.leader = len(c.peers) - 1
		return true
	}
	return false
}

// String implements fmt.Stringer.
func (c *Client) String() string { return fmt.Sprintf("client(%s)", c.clientID) }
