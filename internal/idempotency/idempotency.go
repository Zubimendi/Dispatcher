// Package idempotency centralizes how Dispatcher decides "have I already
// done this?" It backs a single principle: retries and duplicate client
// requests must never create duplicate side effects.
//
// The mechanism is a real Postgres UNIQUE constraint on
// (tenant_id, idempotency_key), not an application-level "check then
// insert" - a check-then-insert has a race window between two concurrent
// requests with the same key. The constraint is the source of truth;
// application code just has to interpret the resulting error correctly.
package idempotency

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrDuplicate is returned by callers (see internal/outbox) when an insert
// violates the idempotency unique constraint. Callers should treat this as
// success, not failure: fetch and return the original record.
var ErrDuplicate = errors.New("idempotency: duplicate request")

// IsUniqueViolation inspects a pgx error to see if it is the specific
// unique-constraint violation we use for idempotency (Postgres error code
// 23505). Any other error is passed through unchanged.
func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}
