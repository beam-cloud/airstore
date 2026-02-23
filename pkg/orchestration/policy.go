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
	DefaultRetryMaxAttempts = 2
	DefaultRetryDelayMs     = 0
)

func DefaultRunExecutionPolicy() RunExecutionPolicy {
	retry := RunRetryPolicy{
		MaxAttempts: DefaultRetryMaxAttempts,
		DelayMs:     DefaultRetryDelayMs,
	}
	return RunExecutionPolicy{
		Host:            ExecHostSandbox,
		Security:        ExecSecurityAllowlist,
		Ask:             ExecAskOff,
		RuntimeType:     RuntimeTypeGvisor,
		WorkspaceAccess: WorkspaceAccessRW,
		NetworkEnabled:  true,
		Interactive:     false,
		Resources:       map[string]any{},
		Retry:           &retry,
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
		p.RuntimeType = RuntimeTypeGvisor
	}
	if p.WorkspaceAccess == "" {
		p.WorkspaceAccess = WorkspaceAccessRW
	}
	retry := RetryPolicyOrDefault(p.Retry)

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
	case RuntimeTypeGvisor, RuntimeTypeRunc:
	default:
		return fmt.Errorf("invalid runtime_type: %s", p.RuntimeType)
	}
	switch p.WorkspaceAccess {
	case WorkspaceAccessNone, WorkspaceAccessRO, WorkspaceAccessRW:
	default:
		return fmt.Errorf("invalid workspace_access: %s", p.WorkspaceAccess)
	}
	if retry.MaxAttempts <= 0 {
		return fmt.Errorf("invalid retry.max_attempts: %d", retry.MaxAttempts)
	}
	if retry.DelayMs < 0 {
		return fmt.Errorf("invalid retry.delay_ms: %d", retry.DelayMs)
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
		p.RuntimeType = RuntimeTypeGvisor
	}
	if p.WorkspaceAccess == "" {
		p.WorkspaceAccess = WorkspaceAccessRW
	}
	if p.Resources == nil {
		p.Resources = map[string]any{}
	}
	retry := RetryPolicyOrDefault(p.Retry)
	p.Retry = &retry
	return p
}

func NormalizeRunRetryPolicy(r RunRetryPolicy) RunRetryPolicy {
	if r.MaxAttempts <= 0 {
		r.MaxAttempts = DefaultRetryMaxAttempts
	}
	if r.DelayMs < 0 {
		r.DelayMs = DefaultRetryDelayMs
	}
	return r
}

func RetryPolicyOrDefault(r *RunRetryPolicy) RunRetryPolicy {
	if r == nil {
		return NormalizeRunRetryPolicy(RunRetryPolicy{})
	}
	return NormalizeRunRetryPolicy(*r)
}

func ToRunExecutionType(p RunExecutionPolicy) types.RunExecutionType {
	if p.Interactive {
		return types.RunExecutionTypeInteractive
	}
	return types.RunExecutionTypeBackground
}

func ToRunExecutionResources(p RunExecutionPolicy) *types.RunExecutionResources {
	if len(p.Resources) == 0 {
		return nil
	}
	out := &types.RunExecutionResources{}
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
