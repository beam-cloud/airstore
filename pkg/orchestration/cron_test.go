package orchestration

import (
	"testing"
	"time"
)

func TestNextCronTime_UTC(t *testing.T) {
	// 9 AM UTC cron, ref time is 8 AM UTC -> next should be 9 AM UTC same day
	ref := time.Date(2026, 3, 3, 8, 0, 0, 0, time.UTC)
	next, err := NextCronTime("0 9 * * *", ref, "UTC")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := time.Date(2026, 3, 3, 9, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, next)
	}
}

func TestNextCronTime_NewYork(t *testing.T) {
	nyc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("timezone data not available: %v", err)
	}

	// 9 AM cron in New York, ref time is 8 AM Eastern
	ref := time.Date(2026, 3, 3, 8, 0, 0, 0, nyc)
	next, err := NextCronTime("0 9 * * *", ref, "America/New_York")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := time.Date(2026, 3, 3, 9, 0, 0, 0, nyc)
	if !next.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, next)
	}

	// Verify it's NOT 9 AM UTC -- it should be 14:00 UTC (EST = UTC-5)
	if next.UTC().Hour() == 9 {
		t.Fatalf("should not be 9 AM UTC, expected 14:00 UTC, got %v", next.UTC())
	}
}

func TestNextCronTime_DifferentTimezonesSameExpr(t *testing.T) {
	// Same cron "0 9 * * *" but different timezones should produce different absolute times
	ref := time.Date(2026, 3, 3, 0, 0, 0, 0, time.UTC)

	utcNext, err := NextCronTime("0 9 * * *", ref, "UTC")
	if err != nil {
		t.Fatalf("UTC: unexpected error: %v", err)
	}

	nycNext, err := NextCronTime("0 9 * * *", ref, "America/New_York")
	if err != nil {
		t.Fatalf("NYC: unexpected error: %v", err)
	}

	if utcNext.Equal(nycNext) {
		t.Fatalf("9 AM UTC and 9 AM New York should be different absolute times, both got %v", utcNext)
	}

	// 9 AM NYC is later in the day UTC-wise (14:00 UTC in EST)
	if !nycNext.After(utcNext) {
		t.Fatalf("9 AM NYC (%v) should be after 9 AM UTC (%v) in absolute time", nycNext, utcNext)
	}
}

func TestNextCronTime_InvalidTimezone_FallsBackToUTC(t *testing.T) {
	ref := time.Date(2026, 3, 3, 8, 0, 0, 0, time.UTC)

	next, err := NextCronTime("0 9 * * *", ref, "Invalid/Timezone")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := time.Date(2026, 3, 3, 9, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("invalid timezone should fall back to UTC; expected %v, got %v", expected, next)
	}
}

func TestNextCronTime_EmptyTimezone_FallsBackToUTC(t *testing.T) {
	ref := time.Date(2026, 3, 3, 8, 0, 0, 0, time.UTC)

	next, err := NextCronTime("0 9 * * *", ref, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := time.Date(2026, 3, 3, 9, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("empty timezone should fall back to UTC; expected %v, got %v", expected, next)
	}
}
