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
// next fire time after ref.
func NextCronTime(expr string, ref time.Time) (time.Time, error) {
	sched, err := cronParser.Parse(expr)
	if err != nil {
		return time.Time{}, err
	}
	return sched.Next(ref), nil
}

// resolveCronExpr accepts either a standard 5-field cron expression or a
// natural language description. If the input doesn't parse as cron, it calls
// BAML to convert the natural language into a cron expression.
func resolveCronExpr(ctx context.Context, input string) (string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", fmt.Errorf("cron_expr is required")
	}
	if _, err := cronParser.Parse(input); err == nil {
		return input, nil
	}
	result, err := baml.ParseCronSchedule(ctx, input)
	if err != nil {
		return "", fmt.Errorf("could not parse schedule %q: %w", input, err)
	}
	resolved := strings.TrimSpace(result.Cron_expr)
	if _, err := cronParser.Parse(resolved); err != nil {
		return "", fmt.Errorf("LLM returned invalid cron %q for %q: %w", resolved, input, err)
	}
	log.Info().Str("input", input).Str("cron", resolved).Msg("resolved natural language to cron")
	return resolved, nil
}
