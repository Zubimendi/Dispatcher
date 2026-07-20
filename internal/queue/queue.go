// Package queue implements a Postgres-native job queue using
// `SELECT ... FOR UPDATE SKIP LOCKED`. This is the concurrency-control
// principle in action: many worker processes (potentially dozens, running
// on different machines) can poll the same delivery_jobs table and each
// row will be claimed by exactly one worker, with no external broker.
//
// Why not Kafka/SQS here: the queue IS the source of truth for delivery
// state (see pkg/models.JobStatus), and that state has to be
// transactionally consistent with the state machine transitions in
// internal/delivery. Keeping it in Postgres means job claims, state
// transitions, and attempt logging can all happen with the same
// consistency guarantees, with no separate system to keep in sync.
// (A production system might front this with Kafka/NATS for the initial
// event ingress at very high volume - this design optimizes for
// correctness and operational simplicity over raw throughput.)
package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yourname/dispatcher/pkg/models"
)

type Queue struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Queue {
	return &Queue{pool: pool}
}

// Claim atomically finds one PENDING job whose next_attempt_at has
// elapsed, locks it (SKIP LOCKED means a job already locked by another
// worker is simply skipped, not blocked on), and marks it DELIVERING.
// Returns (nil, nil) if there is no claimable work right now - that is
// the normal, expected "queue is empty" case, not an error.
func (q *Queue) Claim(ctx context.Context, workerID string) (*models.DeliveryJob, error) {
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var job models.DeliveryJob
	err = tx.QueryRow(ctx, `
		SELECT id, event_id, endpoint_id, status, attempt_count, max_attempts, next_attempt_at
		FROM delivery_jobs
		WHERE status = $1 AND next_attempt_at <= now()
		ORDER BY next_attempt_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`, models.JobPending).Scan(&job.ID, &job.EventID, &job.EndpointID, &job.Status,
		&job.AttemptCount, &job.MaxAttempts, &job.NextAttemptAt)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("claim job: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE delivery_jobs
		SET status = $1, locked_by = $2, locked_at = now(), updated_at = now()
		WHERE id = $3
	`, models.JobDelivering, workerID, job.ID)
	if err != nil {
		return nil, fmt.Errorf("lock job: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}

	job.Status = models.JobDelivering
	return &job, nil
}

// MarkSucceeded transitions a job to its terminal success state and
// records the attempt. One transaction: state + audit log together.
func (q *Queue) MarkSucceeded(ctx context.Context, jobID string, attemptNumber, httpStatus, durationMS int) error {
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		UPDATE delivery_jobs SET status = $1, updated_at = now() WHERE id = $2
	`, models.JobSucceeded, jobID); err != nil {
		return fmt.Errorf("mark succeeded: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO delivery_attempt_logs (delivery_job_id, attempt_number, status, http_status, duration_ms)
		VALUES ($1, $2, $3, $4, $5)
	`, jobID, attemptNumber, models.AttemptSucceeded, httpStatus, durationMS); err != nil {
		return fmt.Errorf("log attempt: %w", err)
	}

	return tx.Commit(ctx)
}

// MarkFailed increments the attempt count and either reschedules the job
// (with exponential backoff, computed by the caller) or dead-letters it if
// attempts are exhausted. Also logs the attempt for the audit trail.
func (q *Queue) MarkFailed(ctx context.Context, jobID string, attemptNumber int, nextAttemptAt time.Time, exhausted bool, httpStatus *int, errMsg string, durationMS int) error {
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	status := models.JobPending
	if exhausted {
		status = models.JobDeadLettered
	}

	if _, err := tx.Exec(ctx, `
		UPDATE delivery_jobs
		SET status = $1, attempt_count = $2, next_attempt_at = $3,
		    last_error = $4, locked_by = NULL, locked_at = NULL, updated_at = now()
		WHERE id = $5
	`, status, attemptNumber, nextAttemptAt, errMsg, jobID); err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO delivery_attempt_logs (delivery_job_id, attempt_number, status, http_status, error, duration_ms)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, jobID, attemptNumber, models.AttemptFailed, httpStatus, errMsg, durationMS); err != nil {
		return fmt.Errorf("log attempt: %w", err)
	}

	return tx.Commit(ctx)
}

// ReleaseStale reclaims jobs stuck in DELIVERING because a worker crashed
// mid-flight without ever calling MarkSucceeded/MarkFailed. This is the
// explicit failure path for "the consumer itself died" - without it, a
// crashed worker would leak jobs forever in an unclaimable state.
func (q *Queue) ReleaseStale(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := q.pool.Exec(ctx, `
		UPDATE delivery_jobs
		SET status = $1, locked_by = NULL, locked_at = NULL, updated_at = now()
		WHERE status = $2 AND locked_at < now() - $3::interval
	`, models.JobPending, models.JobDelivering, olderThan.String())
	if err != nil {
		return 0, fmt.Errorf("release stale jobs: %w", err)
	}
	return tag.RowsAffected(), nil
}
