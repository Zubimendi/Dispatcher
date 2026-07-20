// Package reconcile answers the question every financial/metering system
// must be able to answer: "what actually happened?"
//
// "If money is involved, treat reconciliation as a core workflow."
// Delivery success is recorded in delivery_attempt_logs by the worker.
// Billing usage is recorded independently in usage_ledger by a subscriber
// to the "delivery.succeeded" domain event (see internal/events). Because
// that hand-off is asynchronous and at-least-once, the two can drift:
// the subscriber could be down when an event publishes, or could process
// a duplicate. This job compares them and produces a durable report a
// human (or an alert) can act on, rather than silently trusting that
// "eventually consistent" actually converged.
package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/yourname/dispatcher/internal/observability"
)

type Reconciler struct {
	pool *pgxpool.Pool
	log  *zap.Logger
}

func New(pool *pgxpool.Pool, log *zap.Logger) *Reconciler {
	return &Reconciler{pool: pool, log: log}
}

type Report struct {
	DeliveredCount     int
	LedgerCount        int
	MissingFromLedger  []string // event_ids delivered but never billed
	OrphanedInLedger   []string // event_ids billed but no successful delivery on record
}

func (r *Reconciler) Run(ctx context.Context, since time.Time, until time.Time) (Report, error) {
	var report Report

	deliveredRows, err := r.pool.Query(ctx, `
		SELECT dj.event_id
		FROM delivery_attempt_logs al
		JOIN delivery_jobs dj ON dj.id = al.delivery_job_id
		WHERE al.status = 'SUCCEEDED' AND al.attempted_at >= $1 AND al.attempted_at < $2
	`, since, until)
	if err != nil {
		return report, fmt.Errorf("query delivered: %w", err)
	}
	delivered := map[string]bool{}
	for deliveredRows.Next() {
		var id string
		deliveredRows.Scan(&id)
		delivered[id] = true
	}
	deliveredRows.Close()
	report.DeliveredCount = len(delivered)

	ledgerRows, err := r.pool.Query(ctx, `
		SELECT event_id FROM usage_ledger WHERE delivered_at >= $1 AND delivered_at < $2
	`, since, until)
	if err != nil {
		return report, fmt.Errorf("query ledger: %w", err)
	}
	billed := map[string]bool{}
	for ledgerRows.Next() {
		var id string
		ledgerRows.Scan(&id)
		billed[id] = true
	}
	ledgerRows.Close()
	report.LedgerCount = len(billed)

	for id := range delivered {
		if !billed[id] {
			report.MissingFromLedger = append(report.MissingFromLedger, id)
		}
	}
	for id := range billed {
		if !delivered[id] {
			report.OrphanedInLedger = append(report.OrphanedInLedger, id)
		}
	}

	mismatchCount := len(report.MissingFromLedger) + len(report.OrphanedInLedger)
	observability.ReconciliationMismatches.Set(float64(mismatchCount))

	details, _ := json.Marshal(map[string]interface{}{
		"missingFromLedger": report.MissingFromLedger,
		"orphanedInLedger":  report.OrphanedInLedger,
	})
	_, err = r.pool.Exec(ctx, `
		INSERT INTO reconciliation_reports
			(period_start, period_end, delivered_count, ledger_count, missing_from_ledger, orphaned_in_ledger, details)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, since, until, report.DeliveredCount, report.LedgerCount,
		len(report.MissingFromLedger), len(report.OrphanedInLedger), details)
	if err != nil {
		return report, fmt.Errorf("persist report: %w", err)
	}

	if mismatchCount > 0 {
		r.log.Warn("reconciliation found mismatches",
			zap.Int("missing_from_ledger", len(report.MissingFromLedger)),
			zap.Int("orphaned_in_ledger", len(report.OrphanedInLedger)))
	} else {
		r.log.Info("reconciliation clean", zap.Int("delivered", report.DeliveredCount))
	}

	return report, nil
}
