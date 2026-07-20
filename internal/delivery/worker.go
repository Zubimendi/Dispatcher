// Worker claims jobs from the queue and drives them through the delivery
// state machine. This file is where most of the resilience principles
// converge: timeouts, exponential backoff with jitter, circuit breaking,
// idempotent consumption, and explicit failure paths (dead-lettering).
package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/yourname/dispatcher/internal/events"
	"github.com/yourname/dispatcher/internal/observability"
	"github.com/yourname/dispatcher/internal/queue"
	"github.com/yourname/dispatcher/pkg/models"
)

type Worker struct {
	id       string
	pool     *pgxpool.Pool
	q        *queue.Queue
	breaker  *CircuitBreaker
	bus      *events.Bus
	log      *zap.Logger
	client   *http.Client
}

func NewWorker(id string, pool *pgxpool.Pool, q *queue.Queue, breaker *CircuitBreaker, bus *events.Bus, log *zap.Logger, timeout time.Duration) *Worker {
	return &Worker{
		id:      id,
		pool:    pool,
		q:       q,
		breaker: breaker,
		bus:     bus,
		log:     log,
		// A dedicated http.Client per worker with an explicit timeout.
		// "Network I/O is unpredictable. Your request lifecycle
		// shouldn't be." Without this, one slow customer endpoint could
		// hold a worker goroutine (and its DB connection) hostage
		// indefinitely.
		client: &http.Client{Timeout: timeout},
	}
}

// Run polls for work until ctx is cancelled. Called once per worker
// goroutine; cmd/worker/main.go starts N of these for concurrency.
func (w *Worker) Run(ctx context.Context, pollInterval time.Duration) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.log.Info("worker shutting down", zap.String("worker_id", w.id))
			return
		case <-ticker.C:
			w.tick(ctx)
		}
	}
}

func (w *Worker) tick(ctx context.Context) {
	job, err := w.q.Claim(ctx, w.id)
	if err != nil {
		w.log.Error("claim failed", zap.Error(err))
		return
	}
	if job == nil {
		return // nothing to do, expected steady state
	}
	w.process(ctx, job)
}

func (w *Worker) process(ctx context.Context, job *models.DeliveryJob) {
	endpoint, event, err := w.loadContext(ctx, job)
	if err != nil {
		w.log.Error("load job context failed", zap.String("job_id", job.ID), zap.Error(err))
		return
	}

	allowed, err := w.breaker.Allow(ctx, endpoint.ID)
	if err != nil {
		w.log.Error("circuit breaker check failed", zap.Error(err))
		return
	}
	if !allowed {
		// Endpoint's circuit is OPEN: don't attempt delivery, just
		// reschedule for a later probe. This is what prevents a broken
		// customer endpoint from creating a retry storm against itself.
		w.reschedule(ctx, job, "circuit breaker open", nil, 0, false)
		return
	}

	attemptNumber := job.AttemptCount + 1
	body, _ := json.Marshal(map[string]interface{}{
		"id":        event.ID,
		"type":      event.EventType,
		"createdAt": event.CreatedAt,
		"data":      json.RawMessage(event.Payload),
	})

	start := time.Now()
	httpStatus, deliveryErr := w.deliver(ctx, endpoint.URL, endpoint.Secret, body)
	durationMS := int(time.Since(start).Milliseconds())
	observability.DeliveryLatency.Observe(float64(durationMS))

	success := deliveryErr == nil && httpStatus >= 200 && httpStatus < 300

	if breakerErr := w.breaker.RecordResult(ctx, endpoint.ID, success); breakerErr != nil {
		w.log.Error("record circuit breaker result failed", zap.Error(breakerErr))
	}

	if success {
		if err := w.q.MarkSucceeded(ctx, job.ID, attemptNumber, httpStatus, durationMS); err != nil {
			w.log.Error("mark succeeded failed", zap.Error(err))
			return
		}
		observability.DeliveryAttempts.WithLabelValues("succeeded").Inc()

		// Publish domain event for other bounded contexts (billing/usage).
		payload, _ := json.Marshal(map[string]string{
			"eventId": event.ID, "endpointId": endpoint.ID, "tenantId": event.TenantID,
		})
		w.bus.Publish(ctx, events.DomainEvent{Type: "delivery.succeeded", Payload: payload})
		return
	}

	errMsg := "non-2xx response"
	if deliveryErr != nil {
		errMsg = deliveryErr.Error()
	}
	var httpStatusPtr *int
	if httpStatus > 0 {
		httpStatusPtr = &httpStatus
	}
	exhausted := attemptNumber >= job.MaxAttempts
	w.reschedule(ctx, job, errMsg, httpStatusPtr, durationMS, exhausted)

	if exhausted {
		observability.DeliveryAttempts.WithLabelValues("dead_lettered").Inc()
		payload, _ := json.Marshal(map[string]string{
			"eventId": event.ID, "endpointId": endpoint.ID, "tenantId": event.TenantID, "error": errMsg,
		})
		w.bus.Publish(ctx, events.DomainEvent{Type: "delivery.dead_lettered", Payload: payload})
	} else {
		observability.DeliveryAttempts.WithLabelValues("failed").Inc()
	}
}

func (w *Worker) reschedule(ctx context.Context, job *models.DeliveryJob, errMsg string, httpStatus *int, durationMS int, exhausted bool) {
	attemptNumber := job.AttemptCount + 1
	backoff := backoffWithJitter(attemptNumber)
	next := time.Now().Add(backoff)
	if err := w.q.MarkFailed(ctx, job.ID, attemptNumber, next, exhausted, httpStatus, errMsg, durationMS); err != nil {
		w.log.Error("mark failed failed", zap.Error(err))
	}
}

// backoffWithJitter: exponential backoff (base 2s, capped at ~10min) with
// full jitter, so many jobs failing at once (e.g. one endpoint going
// down) don't all retry in lockstep and hammer it the moment the window
// opens - a "thundering herd" the naive version of this feature creates.
func backoffWithJitter(attempt int) time.Duration {
	base := 2 * time.Second
	max := 10 * time.Minute
	backoff := time.Duration(math.Min(
		float64(base)*math.Pow(2, float64(attempt-1)),
		float64(max),
	))
	jitter := time.Duration(rand.Int63n(int64(backoff) / 2))
	return backoff/2 + jitter
}

func (w *Worker) deliver(ctx context.Context, url, secret string, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Dispatcher-Signature", Sign(secret, body))

	resp, err := w.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) // drain so the connection can be reused

	return resp.StatusCode, nil
}

type endpointRow struct {
	ID     string
	URL    string
	Secret string
}

func (w *Worker) loadContext(ctx context.Context, job *models.DeliveryJob) (endpointRow, eventRow, error) {
	var ep endpointRow
	err := w.pool.QueryRow(ctx, `SELECT id, url, secret FROM endpoints WHERE id = $1`, job.EndpointID).
		Scan(&ep.ID, &ep.URL, &ep.Secret)
	if err != nil {
		return ep, eventRow{}, fmt.Errorf("load endpoint: %w", err)
	}

	var ev eventRow
	err = w.pool.QueryRow(ctx, `SELECT id, tenant_id, event_type, payload, created_at FROM events WHERE id = $1`, job.EventID).
		Scan(&ev.ID, &ev.TenantID, &ev.EventType, &ev.Payload, &ev.CreatedAt)
	if err != nil {
		return ep, ev, fmt.Errorf("load event: %w", err)
	}
	return ep, ev, nil
}

type eventRow struct {
	ID        string
	TenantID  string
	EventType string
	Payload   []byte
	CreatedAt time.Time
}
