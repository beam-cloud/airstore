package company

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/google/uuid"
)

// ActionExecutor composes existing APIs into high-level company actions.
type ActionExecutor struct {
	agentAPI *orchestration.AgentAPI
	backend  repository.BackendRepository
}

func NewActionExecutor(
	agentAPI *orchestration.AgentAPI,
	backend repository.BackendRepository,
) *ActionExecutor {
	return &ActionExecutor{agentAPI: agentAPI, backend: backend}
}

// Execute runs a single CompanyAction and returns the result.
func (e *ActionExecutor) Execute(ctx context.Context, workspaceID uint, action CompanyAction) ActionResult {
	result := ActionResult{
		Action:    action,
		Status:    ActionStatusPending,
		Timestamp: nowMs(),
	}

	var resourceIDs []string
	var err error

	switch action.Type {
	case ActionTypeProvisionAgent:
		resourceIDs, err = e.provisionAgent(ctx, workspaceID, action.Params)
	case ActionTypeModifyAgent:
		resourceIDs, err = e.modifyAgent(ctx, workspaceID, action.Params)
	case ActionTypeCreateTask:
		resourceIDs, err = e.createTask(ctx, workspaceID, action.Params)
	case ActionTypeLaunchCampaign:
		resourceIDs, err = e.launchCampaign(ctx, workspaceID, action.Params)
	case ActionTypeCreateSchedule:
		resourceIDs, err = e.createSchedule(ctx, workspaceID, action.Params)
	default:
		err = fmt.Errorf("unknown action type: %s", action.Type)
	}

	if err != nil {
		result.Status = ActionStatusError
		result.Error = err.Error()
	} else {
		result.Status = ActionStatusSuccess
		result.ResourceIDs = resourceIDs
	}
	result.Timestamp = nowMs()
	return result
}

// ExecuteAll runs a batch of actions and returns all results.
func (e *ActionExecutor) ExecuteAll(ctx context.Context, workspaceID uint, actions []CompanyAction) []ActionResult {
	results := make([]ActionResult, 0, len(actions))
	for _, action := range actions {
		results = append(results, e.Execute(ctx, workspaceID, action))
	}
	return results
}

func (e *ActionExecutor) provisionAgent(ctx context.Context, workspaceID uint, p ActionParams) ([]string, error) {
	key := p.AgentKey
	if key == "" {
		key = strings.ReplaceAll(strings.ToLower(p.AgentName), " ", "-")
	}
	name := p.AgentName
	if name == "" {
		return nil, fmt.Errorf("agent_name is required")
	}

	config := make(map[string]any)
	if p.Model != "" {
		config["model"] = p.Model
	}
	if p.SystemPrompt != "" {
		config["system_prompt"] = p.SystemPrompt
	}
	if len(p.Skills) > 0 {
		config["skills"] = p.Skills
	}

	agent, err := e.agentAPI.CreateAgent(ctx, workspaceID, key, name, config, p.Active)
	if err != nil {
		return nil, fmt.Errorf("create agent: %w", err)
	}

	return []string{agent.ID}, nil
}

func (e *ActionExecutor) modifyAgent(ctx context.Context, workspaceID uint, p ActionParams) ([]string, error) {
	if p.AgentID == "" {
		return nil, fmt.Errorf("agent_id is required for modify_agent")
	}

	var namePtr, rolePtr *string
	if p.AgentName != "" {
		namePtr = &p.AgentName
	}
	if p.AgentRole != "" {
		rolePtr = &p.AgentRole
	}

	config := make(map[string]any)
	if p.Model != "" {
		config["model"] = p.Model
	}
	if p.SystemPrompt != "" {
		config["system_prompt"] = p.SystemPrompt
	}
	if len(p.Skills) > 0 {
		config["skills"] = p.Skills
	}

	var cfgArg map[string]any
	if len(config) > 0 {
		cfgArg = config
	}

	agent, err := e.agentAPI.UpdateAgent(ctx, workspaceID, p.AgentID, namePtr, rolePtr, nil, nil, nil, cfgArg, p.Active)
	if err != nil {
		return nil, fmt.Errorf("update agent: %w", err)
	}
	return []string{agent.ID}, nil
}

func (e *ActionExecutor) createTask(ctx context.Context, workspaceID uint, p ActionParams) ([]string, error) {
	agentID := p.AgentID
	if agentID == "" && len(p.TargetAgentIDs) > 0 {
		agentID = p.TargetAgentIDs[0]
	}
	if agentID == "" {
		return nil, fmt.Errorf("agent_id is required for create_task")
	}

	message := p.Message
	if message == "" {
		return nil, fmt.Errorf("message is required for create_task")
	}

	priority := p.Priority
	if priority == "" {
		priority = "normal"
	}

	sessionID := uuid.New().String()
	idempotencyKey := uuid.New().String()

	task, _, err := e.agentAPI.AcceptAgentCommand(ctx, workspaceID, orchestration.AgentCommandParams{
		Message:        message,
		AgentID:        &agentID,
		SessionID:      sessionID,
		IdempotencyKey: idempotencyKey,
		Priority:       priority,
	})
	if err != nil {
		return nil, fmt.Errorf("create task: %w", err)
	}
	return []string{task.ID}, nil
}

func (e *ActionExecutor) launchCampaign(ctx context.Context, workspaceID uint, p ActionParams) ([]string, error) {
	if p.Message == "" && p.PromptTemplate == "" {
		return nil, fmt.Errorf("message or prompt_template is required")
	}
	if len(p.TargetAgentIDs) == 0 {
		return nil, fmt.Errorf("target_agent_ids is required for launch_campaign")
	}

	count := p.Count
	if count <= 0 {
		count = 1
	}

	prompt := p.Message
	if prompt == "" {
		prompt = p.PromptTemplate
	}

	var taskIDs []string
	for _, agentID := range p.TargetAgentIDs {
		for i := 0; i < count; i++ {
			sessionID := uuid.New().String()
			idempotencyKey := uuid.New().String()
			aid := agentID

			task, _, err := e.agentAPI.AcceptAgentCommand(ctx, workspaceID, orchestration.AgentCommandParams{
				Message:        prompt,
				AgentID:        &aid,
				SessionID:      sessionID,
				IdempotencyKey: idempotencyKey,
				Priority:       p.Priority,
			})
			if err != nil {
				return taskIDs, fmt.Errorf("create task for agent %s: %w", agentID, err)
			}
			taskIDs = append(taskIDs, task.ID)
		}
	}
	return taskIDs, nil
}

func (e *ActionExecutor) createSchedule(ctx context.Context, workspaceID uint, p ActionParams) ([]string, error) {
	if p.AgentID == "" {
		return nil, fmt.Errorf("agent_id is required for create_schedule")
	}
	if p.CronExpr == "" {
		return nil, fmt.Errorf("cron_expr is required for create_schedule")
	}
	if p.Message == "" {
		return nil, fmt.Errorf("message is required for create_schedule")
	}

	tz := p.Timezone
	if tz == "" {
		tz = "UTC"
	}

	schedule, err := e.agentAPI.CreateSchedule(
		ctx, workspaceID,
		p.AgentID, p.CronExpr, tz, p.Message,
		p.SkillPaths, nil, nil, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("create schedule: %w", err)
	}
	return []string{schedule.ExternalID}, nil
}
