package delivery

import (
	"testing"

	"github.com/yourname/dispatcher/pkg/models"
)

func TestCanTransition_LegalPaths(t *testing.T) {
	cases := []struct {
		from, to models.JobStatus
		want     bool
	}{
		{models.JobPending, models.JobDelivering, true},
		{models.JobDelivering, models.JobSucceeded, true},
		{models.JobDelivering, models.JobFailed, true},
		{models.JobFailed, models.JobPending, true},
		{models.JobFailed, models.JobDeadLettered, true},
		// illegal: cannot resurrect a terminal state
		{models.JobSucceeded, models.JobDelivering, false},
		{models.JobDeadLettered, models.JobPending, false},
		// illegal: cannot skip straight to delivering->succeeded without
		// ever being claimed
		{models.JobPending, models.JobSucceeded, false},
	}

	for _, c := range cases {
		got := CanTransition(c.from, c.to)
		if got != c.want {
			t.Errorf("CanTransition(%s, %s) = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestValidateTransition_ReturnsErrorOnIllegalMove(t *testing.T) {
	if err := ValidateTransition(models.JobSucceeded, models.JobDelivering); err == nil {
		t.Fatal("expected error transitioning out of a terminal state, got nil")
	}
}
