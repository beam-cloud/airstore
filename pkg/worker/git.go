package worker

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/runtime"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
)

const (
	gitConfigPath   = "/tmp/airstore-git/config"
	gitSetupTimeout = 15 * time.Second
)

// gitSetupScript runs inside the sandbox to configure git. It:
//  1. Creates the config directory under /tmp (writable inside the container)
//  2. Writes a credential helper that calls the github tool
//  3. Writes a base gitconfig pointing to the credential helper
//  4. Calls the github tool to resolve the real user's name/email
//
// The script always writes the credential helper (steps 1–3). Identity
// resolution (step 4) is best-effort — if it fails the script still exits
// 0 but prints a marker so the caller can log accurately.
const gitSetupScript = `#!/bin/sh
set -e
DIR=/tmp/airstore-git
mkdir -p "$DIR"

cat > "$DIR/credential-helper" <<'CRED'
#!/bin/sh
test "$1" = get || exit 0
TOKEN=$(/workspace/tools/github git-credentials 2>/dev/null)
[ -z "$TOKEN" ] && exit 1
printf 'username=x-access-token\npassword=%s\n' "$TOKEN"
CRED
chmod +x "$DIR/credential-helper"

cat > "$DIR/config" <<'GIT'
[credential "https://github.com"]
	helper = /tmp/airstore-git/credential-helper
GIT

if /workspace/tools/github git-config >> "$DIR/config" 2>/dev/null; then
  echo "git_identity=ok"
else
  echo "git_identity=unavailable"
fi
`

const gitIdentityOKMarker = "git_identity=ok"

// setupGitInsideSandbox execs the git setup script inside a running sandbox.
// It writes the credential helper, gitconfig, and resolves the GitHub user's
// identity — all in a single exec call. Best-effort: failures are logged but
// do not block the task.
func setupGitInsideSandbox(ctx context.Context, rt runtime.Runtime, sandboxID string, env map[string]string) {
	ctx, cancel := context.WithTimeout(ctx, gitSetupTimeout)
	defer cancel()

	var buf bytes.Buffer
	proc := specs.Process{
		Args: []string{"/bin/sh", "-c", gitSetupScript},
		Cwd:  types.ContainerWorkDir,
		Env:  buildGitSetupEnv(env),
		User: specs.User{UID: types.SandboxUserUID, GID: types.SandboxUserGID},
	}

	if err := rt.Exec(ctx, sandboxID, proc, &runtime.ExecOpts{OutputWriter: &buf}); err != nil {
		output := strings.TrimSpace(buf.String())
		log.Warn().Err(err).Str("sandbox_id", sandboxID).Str("output", output).
			Msg("git setup failed")
		return
	}

	if strings.Contains(buf.String(), gitIdentityOKMarker) {
		log.Info().Str("sandbox_id", sandboxID).Msg("git configured with GitHub identity")
	} else {
		log.Info().Str("sandbox_id", sandboxID).Msg("git credential helper configured (GitHub identity unavailable)")
	}
}

// buildGitSetupEnv returns the minimal env needed for the git setup exec.
// The tool shim needs AIRSTORE_TOKEN and GATEWAY_ADDR to call the gateway.
func buildGitSetupEnv(env map[string]string) []string {
	out := []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		fmt.Sprintf("GIT_CONFIG_GLOBAL=%s", gitConfigPath),
	}
	for _, key := range []string{"AIRSTORE_TOKEN", "GATEWAY_ADDR", "HOME"} {
		if v, ok := env[key]; ok && v != "" {
			out = append(out, fmt.Sprintf("%s=%s", key, v))
		}
	}
	return out
}

// gitEnvVars returns the environment variables that point git at the
// config written by setupGitInsideSandbox.
func gitEnvVars() map[string]string {
	return map[string]string{
		"GIT_CONFIG_GLOBAL": gitConfigPath,
	}
}
