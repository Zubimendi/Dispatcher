// Package cache implements cache-aside reads of endpoint configuration
// (used by the GraphQL API's read path, not the worker's critical write
// path - the worker always reads endpoints fresh from Postgres because
// circuit breaker state must be current).
//
// "If the same computation is performed repeatedly, introduce caching."
// "If cached data becomes stale, define an invalidation strategy."
// Here that strategy is explicit invalidate-on-write: any mutation that
// changes an endpoint calls Invalidate in the SAME request path, before
// returning success to the caller. There is no reliance on TTL alone to
// paper over staleness, though a TTL is still set as a safety net for
// invalidation bugs / out-of-band DB writes.
package cache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yourname/dispatcher/pkg/models"
)

const endpointTTL = 5 * time.Minute

type Cache struct {
	rdb *redis.Client
}

func New(addr string) *Cache {
	return &Cache{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

func endpointKey(id string) string { return "endpoint:" + id }

func (c *Cache) GetEndpoint(ctx context.Context, id string) (*models.Endpoint, error) {
	val, err := c.rdb.Get(ctx, endpointKey(id)).Bytes()
	if err == redis.Nil {
		return nil, nil // cache miss, not an error
	}
	if err != nil {
		return nil, err
	}
	var ep models.Endpoint
	if err := json.Unmarshal(val, &ep); err != nil {
		return nil, err
	}
	return &ep, nil
}

func (c *Cache) SetEndpoint(ctx context.Context, ep models.Endpoint) error {
	data, err := json.Marshal(ep)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, endpointKey(ep.ID), data, endpointTTL).Err()
}

// Invalidate must be called by every mutation that changes an endpoint
// row. Called synchronously so a client that just updated an endpoint and
// immediately reads it back never sees stale data - this is what "part of
// system design, not an afterthought" means concretely: invalidation is
// wired into the write path's contract, not bolted on later.
func (c *Cache) InvalidateEndpoint(ctx context.Context, id string) error {
	return c.rdb.Del(ctx, endpointKey(id)).Err()
}
