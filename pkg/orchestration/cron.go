package orchestration

import (
	"context"
	"fmt"
	"strings"
	"time"

	cronlib "github.com/robfig/cron/v3"
	"github.com/rs/zerolog/log"

	baml "github.com/beam-cloud/airstore/pkg/sources/queries/baml_client"
)

var cronParser = cronlib.NewParser(cronlib.Minute | cronlib.Hour | cronlib.Dom | cronlib.Month | cronlib.Dow)

// NextCronTime parses a standard 5-field cron expression and returns the
// next fire time after ref, interpreted in the given IANA timezone.
// An empty or invalid timezone falls back to UTC.
func NextCronTime(expr string, ref time.Time, tz string) (time.Time, error) {
	loc := time.UTC
	if tz != "" {
		if parsed, err := time.LoadLocation(tz); err == nil {
			loc = parsed
		}
	}
	sched, err := cronParser.Parse(expr)
	if err != nil {
		return time.Time{}, err
	}
	return sched.Next(ref.In(loc)), nil
}

// resolveCronExpr accepts either a standard 5-field cron expression or a
// natural language description. If the input doesn't parse as cron, it calls
// BAML to convert the natural language into a cron expression.
// The timezone is forwarded to the LLM so it can interpret relative time
// references (e.g. "9am") in the user's local timezone.
func resolveCronExpr(ctx context.Context, input string, tz string) (string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", fmt.Errorf("cron_expr is required")
	}
	if _, err := cronParser.Parse(input); err == nil {
		return input, nil
	}
	result, err := baml.ParseCronSchedule(ctx, input, tz)
	if err != nil {
		return "", fmt.Errorf("could not parse schedule %q: %w", input, err)
	}
	resolved := strings.TrimSpace(result.Cron_expr)
	if _, err := cronParser.Parse(resolved); err != nil {
		return "", fmt.Errorf("LLM returned invalid cron %q for %q: %w", resolved, input, err)
	}
	log.Info().Str("input", input).Str("cron", resolved).Str("timezone", tz).Msg("resolved natural language to cron")
	return resolved, nil
}
