package company

import (
	"context"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	recentResultLimit = 20
	maxPromptSummary  = 120
)

// BuildSnapshot assembles a holistic CompanySnapshot from the current workspace state.
func BuildSnapshot(
	ctx context.Context,
	workspaceID uint,
	agentAPI *orchestration.AgentAPI,
	backend repository.BackendRepository,
	integrations repository.IntegrationRepository,
) (*CompanySnapshot, error) {
	agents, err := agentAPI.ListAgents(ctx, workspaceID)
	if err != nil {
		return nil, err
	}

	tasks, err := agentAPI.ListTasks(ctx, workspaceID, 500)
	if err != nil {
		return nil, err
	}

	schedules, err := agentAPI.ListSchedules(ctx, workspaceID)
	if err != nil {
		return nil, err
	}

	channels, err := agentAPI.ListChannelBindings(ctx, workspaceID, nil)
	if err != nil {
		return nil, err
	}

	var connections []SourceSummary
	if integrations != nil {
		conns, err := integrations.ListConnections(ctx, workspaceID)
		if err == nil {
			for _, c := range conns {
				status := "connected"
				if c.ExpiresAt != nil && c.ExpiresAt.Before(time.Now()) {
					status = "expired"
				}
				connections = append(connections, SourceSummary{
					IntegrationType: c.IntegrationType,
					Status:          status,
				})
			}
		}
	}

	agentNameMap := make(map[string]string, len(agents))
	for _, a := range agents {
		agentNameMap[a.ID] = a.Name
	}

	agentSummaries := buildAgentSummaries(agents, tasks)
	runningTasks, recentResults := partitionTasks(tasks, agentNameMap)
	scheduleSummaries := buildScheduleSummaries(schedules, agentNameMap)
	channelSummaries := buildChannelSummaries(channels, agentNameMap)
	cost := buildCostSummary(tasks)

	return &CompanySnapshot{
		Agents:         agentSummaries,
		RunningTasks:   runningTasks,
		ScheduledTasks: scheduleSummaries,
		RecentResults:  recentResults,
		Sources:        connections,
		Channels:       channelSummaries,
		CostSummary:    cost,
	}, nil
}

func buildAgentSummaries(agents []*types.AgentProfile, tasks []*types.AgentTask) []AgentSummary {
	taskCounts := make(map[string]int)
	agentStates := make(map[string]AgentDerivedState)
	agentCosts := make(map[string]float64)

	for _, t := range tasks {
		if t.AgentID == nil {
			continue
		}
		aid := *t.AgentID
		agentCosts[aid] += t.CostUSD
		if !t.State.IsTerminal() {
			taskCounts[aid]++
		}
		switch t.State {
		case types.AgentTaskStateRunning:
			agentStates[aid] = AgentDerivedStateWorking
		case types.AgentTaskStateWaiting:
			if agentStates[aid] != AgentDerivedStateWorking {
				agentStates[aid] = AgentDerivedStateWaiting
			}
		case types.AgentTaskStateSleeping:
			if agentStates[aid] != AgentDerivedStateWorking && agentStates[aid] != AgentDerivedStateWaiting {
				agentStates[aid] = AgentDerivedStateSleeping
			}
		case types.AgentTaskStateError:
			if agentStates[aid] != AgentDerivedStateWorking {
				agentStates[aid] = AgentDerivedStateError
			}
		}
	}

	out := make([]AgentSummary, 0, len(agents))
	for _, a := range agents {
		state := agentStates[a.ID]
		if state == "" {
			state = AgentDerivedStateIdle
		}

		var model string
		var skills []string
		var sysPrompt string
		if a.ConfigJSON != nil {
			if m, ok := a.ConfigJSON["model"].(string); ok {
				model = m
			}
			if sp, ok := a.ConfigJSON["system_prompt"].(string); ok {
				sysPrompt = sp
			}
			if sk, ok := a.ConfigJSON["skills"].([]interface{}); ok {
				for _, s := range sk {
					if str, ok := s.(string); ok {
						skills = append(skills, str)
					}
				}
			}
		}

		out = append(out, AgentSummary{
			ID:              a.ID,
			Key:             a.AgentKey,
			Name:            a.Name,
			Role:            a.Role,
			Active:          a.Active,
			State:           state,
			ActiveTaskCount: taskCounts[a.ID],
			TotalCostUSD:    agentCosts[a.ID],
			Model:           model,
			Skills:          skills,
			SystemPrompt:    sysPrompt,
		})
	}
	return out
}

func partitionTasks(tasks []*types.AgentTask, agentNames map[string]string) (running []TaskSummary, recent []TaskResultSummary) {
	now := time.Now()
	for _, t := range tasks {
		var agentID, agentName string
		if t.AgentID != nil {
			agentID = *t.AgentID
			agentName = agentNames[agentID]
		}
		prompt := extractPromptSummary(t.PayloadJSON)

		if !t.State.IsTerminal() {
			dur := 0
			if t.DispatchedAt != nil {
				dur = int(now.Sub(*t.DispatchedAt).Seconds())
			}
			running = append(running, TaskSummary{
				ID:            t.ID,
				AgentID:       agentID,
				AgentName:     agentName,
				State:         string(t.State),
				PromptSummary: prompt,
				Priority:      t.Priority,
				CostUSD:       t.CostUSD,
				CreatedAt:     t.AcceptedAt.UnixMilli(),
				DurationSec:   dur,
			})
		} else if len(recent) < recentResultLimit {
			endedAt := t.UpdatedAt.UnixMilli()
			recent = append(recent, TaskResultSummary{
				ID:        t.ID,
				AgentID:   agentID,
				AgentName: agentName,
				State:     string(t.State),
				Prompt:    prompt,
				CostUSD:   t.CostUSD,
				EndedAt:   endedAt,
			})
		}
	}
	return
}

func buildScheduleSummaries(schedules []*types.ScheduledTask, agentNames map[string]string) []ScheduleSummary {
	out := make([]ScheduleSummary, 0, len(schedules))
	for _, s := range schedules {
		out = append(out, ScheduleSummary{
			ID:        s.ExternalID,
			AgentID:   s.AgentID,
			AgentName: agentNames[s.AgentID],
			CronExpr:  s.CronExpr,
			Timezone:  s.Timezone,
			Prompt:    s.Prompt,
			Active:    s.Active,
			NextRunAt: s.NextRunAt.UnixMilli(),
		})
	}
	return out
}

func buildChannelSummaries(channels []*types.ChannelBinding, agentNames map[string]string) []ChannelSummary {
	out := make([]ChannelSummary, 0, len(channels))
	for _, c := range channels {
		var agentID, agentName string
		if c.AgentID != nil {
			agentID = *c.AgentID
			agentName = agentNames[agentID]
		}
		out = append(out, ChannelSummary{
			AgentID:     agentID,
			AgentName:   agentName,
			ChannelType: c.ChannelType,
			Address:     c.Address,
		})
	}
	return out
}

func buildCostSummary(tasks []*types.AgentTask) CostSummary {
	perAgent := make(map[string]float64)
	total := 0.0
	for _, t := range tasks {
		total += t.CostUSD
		if t.AgentID != nil {
			perAgent[*t.AgentID] += t.CostUSD
		}
	}
	return CostSummary{TotalUSD: total, PerAgentUSD: perAgent}
}

func extractPromptSummary(payload map[string]any) string {
	if payload == nil {
		return ""
	}
	msg, _ := payload["message"].(string)
	msg = strings.TrimSpace(msg)
	if len(msg) > maxPromptSummary {
		return msg[:maxPromptSummary] + "..."
	}
	return msg
}
