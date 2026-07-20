// Package archive moves old, operationally-irrelevant delivery attempt
// logs out of the hot table.
//
// "If data is no longer needed operationally, archive it. Operational
// databases shouldn't become long-term storage." delivery_attempt_logs
// grows without bound (one row per attempt, forever); left unchecked it
// bloats indexes the claimable-jobs query and dashboards depend on. This
// job moves rows older than a retention window into a cold table in
// batches, so a single run never holds a long-lived lock or blows up a
// transaction on a multi-year backlog.
package archive

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

const batchSize = 5000

type Archiver struct {
	pool *pgxpool.Pool
	log  *zap.Logger
}

func New(pool *pgxpool.Pool, log *zap.Logger) *Archiver {
	return &Archiver{pool: pool, log: log}
}

func (a *Archiver) Run(ctx context.Context, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan)
	var total int64

	for {
		tx, err := a.pool.Begin(ctx)
		if err != nil {
			return total, err
		}

		rows, err := tx.Query(ctx, `
			SELECT id FROM delivery_attempt_logs
			WHERE attempted_at < $1
			ORDER BY attempted_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		`, cutoff, batchSize)
		if err != nil {
			tx.Rollback(ctx)
			return total, err
		}
		var ids []string
		for rows.Next() {
			var id string
			rows.Scan(&id)
			ids = append(ids, id)
		}
		rows.Close()

		if len(ids) == 0 {
			tx.Rollback(ctx)
			break
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO archived_delivery_attempt_logs
			SELECT * FROM delivery_attempt_logs WHERE id = ANY($1)
		`, ids); err != nil {
			tx.Rollback(ctx)
			return total, err
		}

		if _, err := tx.Exec(ctx, `
			DELETE FROM delivery_attempt_logs WHERE id = ANY($1)
		`, ids); err != nil {
			tx.Rollback(ctx)
			return total, err
		}

		if err := tx.Commit(ctx); err != nil {
			return total, err
		}

		total += int64(len(ids))
		a.log.Info("archived batch", zap.Int("rows", len(ids)))

		if len(ids) < batchSize {
			break
		}
	}

	return total, nil
}
