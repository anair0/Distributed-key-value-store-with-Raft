package shard

import (
	"context"
	"fmt"

	"github.com/anair0/distributed-key-value-store-with-raft/internal/kv"
)

// Client routes each request to the Raft group that owns its key.
// Like kv.Client, it issues one request at a time.
type Client struct {
	ring   *Ring
	groups map[int]*kv.Client
}

// NewClient creates a sharded client. groups maps group ID to a client for
// that group's servers.
func NewClient(groups map[int]*kv.Client, vnodes int) *Client {
	ids := make([]int, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	return &Client{ring: NewRing(ids, vnodes), groups: groups}
}

// Group returns the group that owns key.
func (c *Client) Group(key string) int { return c.ring.Lookup(key) }

func (c *Client) client(key string) (*kv.Client, error) {
	g := c.ring.Lookup(key)
	cl, ok := c.groups[g]
	if !ok {
		return nil, fmt.Errorf("shard: no client for group %d", g)
	}
	return cl, nil
}

// Get returns the value of key.
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	cl, err := c.client(key)
	if err != nil {
		return nil, false, err
	}
	return cl.Get(ctx, key)
}

// Put sets key to value.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	cl, err := c.client(key)
	if err != nil {
		return err
	}
	return cl.Put(ctx, key, value)
}

// Delete removes key.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	cl, err := c.client(key)
	if err != nil {
		return false, err
	}
	return cl.Delete(ctx, key)
}
