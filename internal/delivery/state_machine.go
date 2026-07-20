// Package delivery contains the delivery job state machine and the worker
// that drives jobs through it.
//
// "If a workflow modifies state, model it as explicit state transitions."
// CanTransition is the single place that decides which transitions are
// legal. Every write to delivery_jobs.status in this codebase goes through
// a path that has already called this function - it is not possible to,
// say, move a SUCCEEDED job back to DELIVERING by accident.
package delivery

import (
	"fmt"

	"github.com/Zubimendi/Dispatcher/pkg/models"
)

var allowedTransitions = map[models.JobStatus][]models.JobStatus{
	models.JobPending:      {models.JobDelivering},
	models.JobDelivering:   {models.JobSucceeded, models.JobFailed, models.JobPending}, // last: stale-lock release
	models.JobFailed:       {models.JobPending, models.JobDeadLettered},
	models.JobSucceeded:    {},
	models.JobDeadLettered: {},
}

func CanTransition(from, to models.JobStatus) bool {
	for _, allowed := range allowedTransitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

func ValidateTransition(from, to models.JobStatus) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("illegal state transition: %s -> %s", from, to)
	}
	return nil
}
