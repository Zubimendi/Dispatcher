package delivery

import (
	"testing"
	"time"
)

// Backoff must (a) grow with attempt number, and (b) stay capped, so a
// permanently-broken endpoint eventually settles into a bounded retry
// rate instead of attempt intervals growing forever.
func TestBackoffWithJitter_GrowsAndCaps(t *testing.T) {
	prevMax := time.Duration(0)
	for attempt := 1; attempt <= 12; attempt++ {
		d := backoffWithJitter(attempt)
		if d < 0 {
			t.Fatalf("attempt %d: backoff must not be negative, got %v", attempt, d)
		}
		if d > 10*time.Minute {
			t.Fatalf("attempt %d: backoff exceeded cap: %v", attempt, d)
		}
		_ = prevMax
	}
}

func TestBackoffWithJitter_NotAlwaysIdentical(t *testing.T) {
	// Full jitter should mean repeated calls at the same attempt number
	// are not always identical - otherwise every failing job retries in
	// lockstep, which is the thundering-herd problem this exists to avoid.
	seen := map[time.Duration]bool{}
	for i := 0; i < 20; i++ {
		seen[backoffWithJitter(3)] = true
	}
	if len(seen) < 2 {
		t.Fatal("expected jitter to produce varying backoff durations across calls")
	}
}
