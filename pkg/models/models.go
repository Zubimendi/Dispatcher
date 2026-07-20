package models

import "time"

type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	APIKey    string    `json:"apiKey"`
	CreatedAt time.Time `json:"createdAt"`
}

type CircuitState string

const (
	CircuitClosed   CircuitState = "CLOSED"
	CircuitOpen     CircuitState = "OPEN"
	CircuitHalfOpen CircuitState = "HALF_OPEN"
)

type Endpoint struct {
	ID                  string       `json:"id"`
	TenantID            string       `json:"tenantId"`
	URL                 string       `json:"url"`
	Secret              string       `json:"-"`
	EventTypes          []string     `json:"eventTypes"`
	IsActive            bool         `json:"isActive"`
	CircuitState        CircuitState `json:"circuitState"`
	CircuitFailureCount int          `json:"circuitFailureCount"`
	Version             int          `json:"version"`
}

type EventStatus string

const (
	EventPending    EventStatus = "PENDING"
	EventDispatched EventStatus = "DISPATCHED"
	EventCompleted  EventStatus = "COMPLETED"
	EventFailed     EventStatus = "FAILED"
)

type Event struct {
	ID              string      `json:"id"`
	TenantID        string      `json:"tenantId"`
	EventType       string      `json:"eventType"`
	Payload         []byte      `json:"payload"`
	IdempotencyKey  string      `json:"idempotencyKey"`
	Status          EventStatus `json:"status"`
	CreatedAt       time.Time   `json:"createdAt"`
}

// JobStatus is the explicit state machine for a delivery job.
// Legal transitions (enforced in internal/delivery/state_machine.go):
//
//   PENDING -> DELIVERING -> SUCCEEDED
//   PENDING -> DELIVERING -> FAILED -> PENDING        (retry, backoff elapsed)
//   PENDING -> DELIVERING -> FAILED -> DEAD_LETTERED  (attempts exhausted)
//
// Any other transition is rejected by the state machine, which is what
// makes "impossible states" (e.g. SUCCEEDED -> DELIVERING) unrepresentable
// at the point where the transition is attempted, not just by convention.
type JobStatus string

const (
	JobPending      JobStatus = "PENDING"
	JobDelivering   JobStatus = "DELIVERING"
	JobSucceeded    JobStatus = "SUCCEEDED"
	JobFailed       JobStatus = "FAILED"
	JobDeadLettered JobStatus = "DEAD_LETTERED"
)

type DeliveryJob struct {
	ID             string
	EventID        string
	EndpointID     string
	Status         JobStatus
	AttemptCount   int
	MaxAttempts    int
	NextAttemptAt  time.Time
	LockedBy       *string
	LastError      *string
}

type AttemptLogStatus string

const (
	AttemptSucceeded AttemptLogStatus = "SUCCEEDED"
	AttemptFailed    AttemptLogStatus = "FAILED"
)

type DeliveryAttemptLog struct {
	ID             string
	DeliveryJobID  string
	AttemptNumber  int
	Status         AttemptLogStatus
	HTTPStatus     *int
	Error          *string
	DurationMS     int
	AttemptedAt    time.Time
}
