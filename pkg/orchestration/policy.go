package orchestration

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	workspaceAccessNone = "none"
	workspaceAccessRO   = "ro"
	workspaceAccessRW   = "rw"
	runtimeTypeGvisor   = "gvisor"
	runtimeTypeRunc     = "runc"
	defaultRetryMax     = 2
)

func DefaultRunExecutionPolicy() RunExecutionPolicy {
	return RunExecutionPolicy{
		Host:            ExecHostSandbox,
		Security:        ExecSecurityAllowlist,
		Ask:             ExecAskOff,
		RuntimeType:     runtimeTypeGvisor,
		WorkspaceAccess: workspaceAccessRW,
		NetworkEnabled:  true,
		Interactive:     false,
		Resources:       map[string]any{},
		Retry: RunRetryPolicy{
			MaxAttempts: defaultRetryMax,
			DelayMs:     0,
		},
	}
}

func ValidateRunExecutionPolicy(p RunExecutionPolicy) error {
	if p.Host == "" {
		p.Host = ExecHostSandbox
	}
	if p.Security == "" {
		p.Security = ExecSecurityAllowlist
	}
	if p.Ask == "" {
		p.Ask = ExecAskOff
	}
	if p.RuntimeType == "" {
		p.RuntimeType = runtimeTypeGvisor
	}
	if p.WorkspaceAccess == "" {
		p.WorkspaceAccess = workspaceAccessRW
	}
	p.Retry = NormalizeRunRetryPolicy(p.Retry)

	switch p.Host {
	case ExecHostSandbox:
	default:
		return fmt.Errorf("invalid host: %s", p.Host)
	}
	switch p.Security {
	case ExecSecurityDeny, ExecSecurityAllowlist, ExecSecurityFull:
	default:
		return fmt.Errorf("invalid security: %s", p.Security)
	}
	switch p.Ask {
	case ExecAskOff, ExecAskOnMiss, ExecAskAlways:
	default:
		return fmt.Errorf("invalid ask: %s", p.Ask)
	}
	switch p.RuntimeType {
	case runtimeTypeGvisor, runtimeTypeRunc:
	default:
		return fmt.Errorf("invalid runtime_type: %s", p.RuntimeType)
	}
	switch p.WorkspaceAccess {
	case workspaceAccessNone, workspaceAccessRO, workspaceAccessRW:
	default:
		return fmt.Errorf("invalid workspace_access: %s", p.WorkspaceAccess)
	}
	if p.Retry.MaxAttempts <= 0 {
		return fmt.Errorf("invalid retry.max_attempts: %d", p.Retry.MaxAttempts)
	}
	if p.Retry.DelayMs < 0 {
		return fmt.Errorf("invalid retry.delay_ms: %d", p.Retry.DelayMs)
	}

	return nil
}

func NormalizeRunExecutionPolicy(p RunExecutionPolicy) RunExecutionPolicy {
	if p.Host == "" {
		p.Host = ExecHostSandbox
	}
	if p.Security == "" {
		p.Security = ExecSecurityAllowlist
	}
	if p.Ask == "" {
		p.Ask = ExecAskOff
	}
	if p.RuntimeType == "" {
		p.RuntimeType = runtimeTypeGvisor
	}
	if p.WorkspaceAccess == "" {
		p.WorkspaceAccess = workspaceAccessRW
	}
	if p.Resources == nil {
		p.Resources = map[string]any{}
	}
	p.Retry = NormalizeRunRetryPolicy(p.Retry)
	return p
}

func NormalizeRunRetryPolicy(r RunRetryPolicy) RunRetryPolicy {
	if r.MaxAttempts <= 0 {
		r.MaxAttempts = defaultRetryMax
	}
	if r.DelayMs < 0 {
		r.DelayMs = 0
	}
	return r
}

func ToTaskType(p RunExecutionPolicy) types.TaskType {
	if p.Interactive {
		return types.TaskTypeInteractive
	}
	return types.TaskTypeBackground
}

func ToTaskResources(p RunExecutionPolicy) *types.TaskResources {
	if len(p.Resources) == 0 {
		return nil
	}
	out := &types.TaskResources{}
	if v, ok := numberAsInt64(p.Resources["cpu"]); ok {
		out.CPU = v
	}
	if v, ok := numberAsInt64(p.Resources["memory"]); ok {
		out.Memory = v
	}
	if v, ok := numberAsInt64(p.Resources["gpu"]); ok {
		out.GPU = int(v)
	}
	if out.CPU == 0 && out.Memory == 0 && out.GPU == 0 {
		return nil
	}
	return out
}

func numberAsInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return t, true
	case float32:
		return int64(t), true
	case float64:
		return int64(t), true
	default:
		return 0, false
	}
}

func ExecutionClassKey(workspaceID uint, agentID, lane *string, p RunExecutionPolicy) string {
	p = NormalizeRunExecutionPolicy(p)
	parts := []string{
		fmt.Sprintf("ws=%d", workspaceID),
		fmt.Sprintf("agent=%s", opt(agentID)),
		fmt.Sprintf("lane=%s", opt(lane)),
		fmt.Sprintf("host=%s", p.Host),
		fmt.Sprintf("security=%s", p.Security),
		fmt.Sprintf("ask=%s", p.Ask),
		fmt.Sprintf("runtime=%s", p.RuntimeType),
		fmt.Sprintf("workspace_access=%s", p.WorkspaceAccess),
		fmt.Sprintf("network=%t", p.NetworkEnabled),
		fmt.Sprintf("interactive=%t", p.Interactive),
		fmt.Sprintf("resources=%s", stableResourceString(p.Resources)),
	}
	raw := strings.Join(parts, "|")
	sum := sha1.Sum([]byte(raw))
	return "execclass_" + hex.EncodeToString(sum[:16])
}

func stableResourceString(m map[string]any) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(fmt.Sprintf("%v", m[k]))
	}
	return b.String()
}

func opt(v *string) string {
	if v == nil || strings.TrimSpace(*v) == "" {
		return "_"
	}
	return strings.TrimSpace(*v)
}
