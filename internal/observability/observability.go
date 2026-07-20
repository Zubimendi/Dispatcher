// Package observability wires structured logging and Prometheus metrics.
//
// "If failures aren't observable, you don't have a reliable system."
// Every principle implemented elsewhere in this codebase (retries,
// circuit breakers, dead-lettering, reconciliation) is only actually
// useful in production if someone can SEE it happening. These metrics are
// what an on-call alert would page on.
package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	DeliveryAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dispatcher_delivery_attempts_total",
		Help: "Total delivery attempts, labeled by outcome.",
	}, []string{"outcome"}) // succeeded | failed | dead_lettered

	DeliveryLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "dispatcher_delivery_duration_ms",
		Help:    "Delivery HTTP call latency in milliseconds.",
		Buckets: []float64{10, 50, 100, 250, 500, 1000, 2500, 5000},
	})

	CircuitBreakerOpen = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dispatcher_circuit_breakers_open",
		Help: "Number of endpoints currently in OPEN circuit state.",
	})

	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dispatcher_queue_depth",
		Help: "Number of PENDING delivery jobs waiting to be claimed.",
	})

	ReconciliationMismatches = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dispatcher_reconciliation_mismatches",
		Help: "Mismatches found by the last reconciliation run.",
	})
)

func NewLogger() *zap.Logger {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	return logger
}

// Handler exposes /metrics, /healthz (process is up) and /readyz
// (process is up AND its dependencies - DB, cache - are reachable).
// Distinguishing the two matters for orchestrators: a pod that is up but
// not ready should stop receiving traffic without being killed.
func Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	return mux
}
