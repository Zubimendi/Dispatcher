// Package events is Dispatcher's own internal domain event bus.
//
// "If an event affects multiple bounded contexts, publish a domain
// event." Delivery (this service's core job) and Billing/Usage (a
// separate bounded context, modeled here by internal/reconcile) should
// not share tables or call each other synchronously. The worker publishes
// "delivery.succeeded" / "delivery.dead_lettered" events; the usage
// subscriber consumes them independently and asynchronously, writing to
// its own table (usage_ledger). The two are eventually consistent by
// design - reconciliation exists precisely because immediate consistency
// was traded away for decoupling and availability.
package events

import (
	"context"
	"encoding/json"
	"sync"

	"go.uber.org/zap"
)

type DomainEvent struct {
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
}

type Handler func(ctx context.Context, evt DomainEvent) error

// Bus is a minimal in-process pub/sub. In a multi-node deployment this
// would be backed by Redis Streams, NATS, or Kafka so subscribers in other
// processes/services receive events too - the Handler signature and the
// "publish, don't call directly" discipline stay the same either way,
// which is the point: bounded contexts depend on an event contract, not
// on each other's internals or on being in the same process.
type Bus struct {
	mu       sync.RWMutex
	handlers map[string][]Handler
	log      *zap.Logger
}

func NewBus(log *zap.Logger) *Bus {
	return &Bus{handlers: make(map[string][]Handler), log: log}
}

func (b *Bus) Subscribe(eventType string, h Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[eventType] = append(b.handlers[eventType], h)
}

// Publish is fire-and-forget from the caller's perspective: a failing
// subscriber never rolls back or blocks the publisher (the delivery
// worker). Subscriber failures are logged and are the subscriber's own
// problem to recover from (e.g. via reconciliation), which is exactly the
// eventual-consistency trade-off this bus exists to make explicit.
func (b *Bus) Publish(ctx context.Context, evt DomainEvent) {
	b.mu.RLock()
	handlers := append([]Handler{}, b.handlers[evt.Type]...)
	b.mu.RUnlock()

	for _, h := range handlers {
		if err := h(ctx, evt); err != nil {
			b.log.Error("domain event handler failed",
				zap.String("event_type", evt.Type), zap.Error(err))
		}
	}
}
