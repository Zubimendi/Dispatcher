// Package outbox implements the transactional outbox pattern: the write
// that records "this business event happened" and the writes that create
// delivery jobs for every subscribed endpoint happen in ONE database
// transaction. If any part fails, all of it rolls back - there is no
// window where an event exists but was never fanned out, or where jobs
// exist for an event that was never actually recorded.
//
// This is the concrete answer to two principles at once:
//   "If an operation spans multiple writes, wrap it in a transaction."
//   "If the operation produces an irreversible side effect, persist
//    intent before execution." (the irreversible effect is the outbound
//    HTTP call a worker makes later; the intent is this transaction)
package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Zubimendi/Dispatcher/internal/idempotency"
	"github.com/Zubimendi/Dispatcher/pkg/models"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

type PublishInput struct {
	TenantID       string
	EventType      string
	Payload        json.RawMessage
	IdempotencyKey string
}

// PublishEvent writes the event and fans it out to every active endpoint
// subscribed to EventType, atomically. Returns the persisted event.
// If IdempotencyKey has already been used by this tenant, it returns the
// ORIGINAL event and (dup=true) rather than an error - callers publishing
// the same logical event twice (e.g. after a client-side retry on a
// timed-out request) get the same result both times, with no duplicate
// deliveries.
func (s *Store) PublishEvent(ctx context.Context, in PublishInput) (event models.Event, dup bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return event, false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if committed

	row := tx.QueryRow(ctx, `
		INSERT INTO events (tenant_id, event_type, payload, idempotency_key)
		VALUES ($1, $2, $3, $4)
		RETURNING id, tenant_id, event_type, payload, idempotency_key, status, created_at
	`, in.TenantID, in.EventType, in.Payload, in.IdempotencyKey)

	err = row.Scan(&event.ID, &event.TenantID, &event.EventType, &event.Payload,
		&event.IdempotencyKey, &event.Status, &event.CreatedAt)

	if err != nil {
		if idempotency.IsUniqueViolation(err) {
			// Duplicate publish. Fetch and return the original - this
			// call must be a no-op from the caller's point of view.
			existing, ferr := s.findByIdempotencyKey(ctx, in.TenantID, in.IdempotencyKey)
			if ferr != nil {
				return event, false, fmt.Errorf("fetch existing event after dup: %w", ferr)
			}
			return existing, true, nil
		}
		return event, false, fmt.Errorf("insert event: %w", err)
	}

	// Fan out: create one delivery_job per active endpoint subscribed to
	// this event type. Still inside the same transaction.
	rows, err := tx.Query(ctx, `
		SELECT id FROM endpoints
		WHERE tenant_id = $1 AND is_active = true AND $2 = ANY(event_types)
	`, in.TenantID, in.EventType)
	if err != nil {
		return event, false, fmt.Errorf("select subscribed endpoints: %w", err)
	}

	var endpointIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return event, false, fmt.Errorf("scan endpoint id: %w", err)
		}
		endpointIDs = append(endpointIDs, id)
	}
	rows.Close()

	batch := &pgx.Batch{}
	for _, epID := range endpointIDs {
		batch.Queue(`
			INSERT INTO delivery_jobs (event_id, endpoint_id)
			VALUES ($1, $2)
			ON CONFLICT (event_id, endpoint_id) DO NOTHING
		`, event.ID, epID)
	}
	if batch.Len() > 0 {
		br := tx.SendBatch(ctx, batch)
		for i := 0; i < batch.Len(); i++ {
			if _, err := br.Exec(); err != nil {
				br.Close()
				return event, false, fmt.Errorf("insert delivery job: %w", err)
			}
		}
		br.Close()
	}

	if _, err := tx.Exec(ctx, `UPDATE events SET status = $1 WHERE id = $2`,
		models.EventDispatched, event.ID); err != nil {
		return event, false, fmt.Errorf("mark event dispatched: %w", err)
	}
	event.Status = models.EventDispatched

	if err := tx.Commit(ctx); err != nil {
		return event, false, fmt.Errorf("commit tx: %w", err)
	}

	return event, false, nil
}

func (s *Store) findByIdempotencyKey(ctx context.Context, tenantID, key string) (models.Event, error) {
	var e models.Event
	err := s.pool.QueryRow(ctx, `
		SELECT id, tenant_id, event_type, payload, idempotency_key, status, created_at
		FROM events WHERE tenant_id = $1 AND idempotency_key = $2
	`, tenantID, key).Scan(&e.ID, &e.TenantID, &e.EventType, &e.Payload,
		&e.IdempotencyKey, &e.Status, &e.CreatedAt)
	return e, err
}
