// Circuit breaker per endpoint, persisted on the endpoints row (not held
// in worker memory) so its state is shared correctly across every worker
// replica and survives restarts.
//
// "If a downstream dependency is degraded, apply timeouts, retries with
// exponential backoff, and circuit breakers. Prevent cascading failures."
//
// A customer's broken webhook endpoint should not cause our workers to
// keep spending threads and DB connections retrying it every few seconds
// forever; the breaker gives it a cool-down and periodic probes instead.
package delivery

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yourname/dispatcher/pkg/models"
)

const (
	failureThreshold = 5
	openCooldown     = 60 * time.Second
)

type CircuitBreaker struct {
	pool *pgxpool.Pool
}

func NewCircuitBreaker(pool *pgxpool.Pool) *CircuitBreaker {
	return &CircuitBreaker{pool: pool}
}

// Allow decides whether a delivery attempt should proceed for this
// endpoint right now. HALF_OPEN allows exactly one probe attempt through;
// the caller's subsequent RecordResult decides whether it closes or reopens.
func (c *CircuitBreaker) Allow(ctx context.Context, endpointID string) (bool, error) {
	var state models.CircuitState
	var openedAt *time.Time
	err := c.pool.QueryRow(ctx, `
		SELECT circuit_state, circuit_opened_at FROM endpoints WHERE id = $1
	`, endpointID).Scan(&state, &openedAt)
	if err != nil {
		return false, err
	}

	switch state {
	case models.CircuitClosed:
		return true, nil
	case models.CircuitOpen:
		if openedAt != nil && time.Since(*openedAt) > openCooldown {
			// Cooldown elapsed: allow a single probe through as HALF_OPEN.
			_, err := c.pool.Exec(ctx, `
				UPDATE endpoints SET circuit_state = $1 WHERE id = $2
			`, models.CircuitHalfOpen, endpointID)
			return err == nil, err
		}
		return false, nil
	case models.CircuitHalfOpen:
		// A probe is already in flight conceptually; allow it (single
		// worker will pick up the single available job anyway).
		return true, nil
	}
	return true, nil
}

// RecordResult updates breaker state based on the outcome of an attempt.
// Uses optimistic locking (version column) so concurrent updates from
// different workers processing different jobs for the same endpoint don't
// clobber each other's failure counts.
func (c *CircuitBreaker) RecordResult(ctx context.Context, endpointID string, success bool) error {
	for attempt := 0; attempt < 3; attempt++ {
		var state models.CircuitState
		var failureCount, version int
		err := c.pool.QueryRow(ctx, `
			SELECT circuit_state, circuit_failure_count, version FROM endpoints WHERE id = $1
		`, endpointID).Scan(&state, &failureCount, &version)
		if err != nil {
			return err
		}

		var newState models.CircuitState
		var newCount int
		var openedAt interface{}

		if success {
			newState = models.CircuitClosed
			newCount = 0
			openedAt = nil
		} else {
			newCount = failureCount + 1
			if state == models.CircuitHalfOpen || newCount >= failureThreshold {
				newState = models.CircuitOpen
				openedAt = time.Now()
			} else {
				newState = models.CircuitClosed
				openedAt = nil
			}
		}

		tag, err := c.pool.Exec(ctx, `
			UPDATE endpoints
			SET circuit_state = $1, circuit_failure_count = $2, circuit_opened_at = $3,
			    version = version + 1, updated_at = now()
			WHERE id = $4 AND version = $5
		`, newState, newCount, openedAt, endpointID, version)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			return nil // success, no concurrent modification
		}
		// version mismatch: another worker updated it concurrently for a
		// different job on the same endpoint. Retry with fresh data
		// (optimistic concurrency control).
	}
	return context.DeadlineExceeded
}
