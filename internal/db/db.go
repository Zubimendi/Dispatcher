// Package db owns the Postgres connection pool. A pooled, stateless
// connection is what lets the API and worker processes scale horizontally:
// no in-process state is required to serve a request or process a job.
package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse db config: %w", err)
	}

	// Keep pool bounds explicit rather than relying on driver defaults -
	// an unbounded pool is a classic way a single slow endpoint (see
	// circuit breaker) takes down the whole service by exhausting DB
	// connections under retry storms.
	cfg.MaxConns = 20
	cfg.MinConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to db: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	return pool, nil
}
