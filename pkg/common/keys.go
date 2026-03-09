package common

import (
	"fmt"

	"github.com/beam-cloud/airstore/pkg/types"
)

var (
	// Filesystem VNode metadata keys (used by FUSE layer)
	filesystemDirAccess  = "airstore:filesystem:dir:access:%s:%s" // pid, name
	filesystemDirContent = "airstore:filesystem:dir:content:%s"   // id
	filesystemFile       = "airstore:filesystem:file:%s:%s"       // pid, name

	// Filesystem store cache keys (used by FilesystemStore)
	fsDirMeta     = "airstore:fs:dir:%s"   // pathHash
	fsFileMeta    = "airstore:fs:file:%s"  // pathHash
	fsSymlink     = "airstore:fs:link:%s"  // pathHash
	fsDirChildren = "airstore:fs:ls:%s"    // pathHash
	fsQueryResult = "airstore:qr:%d:%s"    // workspaceId, pathHash
	fsResultBody  = "airstore:rc:%d:%s:%s" // workspaceId, pathHash, resultId
	fsResultIndex = "airstore:idx:rc:%d:%s"
	fsCompIndex   = "airstore:idx:cc:%d:%s"

	// Session keys
	sessionState = "airstore:session:state:%s" // sessionId
	sessionLock  = "airstore:session:lock:%s"  // sessionId
	sessionIndex = "airstore:session:index"

	// Scheduler keys
	schedulerWorkerState = "airstore:scheduler:worker:state:%s" // workerId
	schedulerWorkerLock  = "airstore:scheduler:worker:lock:%s"  // workerId
	schedulerWorkerIndex = "airstore:scheduler:worker:index"

	// Gateway keys
	gatewayInitLock = "airstore:gateway:init:%s:lock" // name

	// Network keys
	networkIPLock = "airstore:network:ip:lock"
	networkIPPool = "airstore:network:pool"
	networkIPMap  = "airstore:network:mapping"

	// Hook keys
	hookStream        = "airstore:hook:events"
	hookConsumerGroup = "airstore:hook:evaluators"
	hookSeen          = "airstore:hook:seen:%d:%s" // workspaceId, pathHash
	hookPollLock      = "airstore:hook:poll:%s"    // queryExternalId

	// OAuth keys
	oauthSession = "airstore:oauth:session:%s" // sessionId
	oauthState   = "airstore:oauth:state:%s"   // state

	// Run execution queue keys
	runExecutionQueueKey    = "airstore:run_execution:queue:%s"    // pool name
	runExecutionDelayedKey  = "airstore:run_execution:delayed:%s"  // pool name (zset by due timestamp ms)
	runExecutionInFlightKey = "airstore:run_execution:inflight:%s" // pool name
	runExecutionStateKey    = "airstore:run_execution:state:%s"    // runExecutionId
	runExecutionResultKey   = "airstore:run_execution:result:%s"   // runExecutionId
	runExecutionLogsChannel = "airstore:run_execution:logs:%s"     // runExecutionId (pub/sub)
	runExecutionLogsBuffer  = "airstore:run_execution:logs_buf:%s" // runExecutionId

	// Task queue keys (high-level task ingress)
	orchestrationTaskDispatchStream = "airstore:orchestration:task_dispatch:stream"
	orchestrationTaskDispatchGroup  = "airstore:orchestration:task_dispatch:group"
	orchestrationTaskDispatchDLQ    = "airstore:orchestration:task_dispatch:dlq"
	orchestrationRunResultStream    = "airstore:orchestration:run_result:stream"
	orchestrationRunResultGroup     = "airstore:orchestration:run_result:group"
	orchestrationRunResultDLQ       = "airstore:orchestration:run_result:dlq"
	agentAttemptEvents              = "airstore:agent:attempt:events"
	agentRunEventsChannel           = "airstore:agent:run:%s:events"
	agentRunEventsBuffer            = "airstore:agent:run:%s:events:buf"
	agentRunRecoveryLock            = "airstore:agent:run:recovery:lock"
	agentInstanceLock               = "airstore:agent:instance:lock:%s" // instanceKey

	// Terminal IO keys (pub/sub channels)
	terminalInput  = "airstore:terminal:%s:input"  // taskId (wake signal)
	terminalOutput = "airstore:terminal:%s:output" // taskId
	terminalCancel      = "airstore:terminal:%s:cancel"       // taskId

	// Session lease — exclusive ownership of an interactive session.
	sessionLease      = "airstore:session:lease:%d:%s"      // workspaceId, sessionId
	sessionCheckpoint = "airstore:session:checkpoint:%d:%s" // workspaceId, sessionId

	// Run interaction state — backend-owned working/waiting/closed snapshot.
	runInteraction = "airstore:run:interaction:%d:%s" // workspaceId, runId

	// Compression keys — include strategy so each compressor caches independently
	fsCompressedPointer = "airstore:compressed:%d:%s:%s:%s" // workspaceId, pathHash, resultId, strategy
	fsCompressedContent = "airstore:cc:%d:%s:%s:%s"         // workspaceId, pathHash, resultId, strategy
	fsCompressedUsage   = "airstore:cc:usage:%d"            // workspaceId
)

// Keys is the singleton accessor for all Redis key patterns.
var Keys = &redisKeys{}

type redisKeys struct{}

// --- Filesystem keys ---

func (rk *redisKeys) FilesystemDirAccess(pid, name string) string {
	return fmt.Sprintf(filesystemDirAccess, pid, name)
}

func (rk *redisKeys) FilesystemDirContent(id string) string {
	return fmt.Sprintf(filesystemDirContent, id)
}

func (rk *redisKeys) FilesystemFile(pid, name string) string {
	return fmt.Sprintf(filesystemFile, pid, name)
}

// --- Filesystem store cache keys (path args are auto-hashed) ---

func (rk *redisKeys) FsDirMeta(path string) string {
	return fmt.Sprintf(fsDirMeta, types.GeneratePathID(path))
}

func (rk *redisKeys) FsFileMeta(path string) string {
	return fmt.Sprintf(fsFileMeta, types.GeneratePathID(path))
}

func (rk *redisKeys) FsSymlink(path string) string {
	return fmt.Sprintf(fsSymlink, types.GeneratePathID(path))
}

func (rk *redisKeys) FsDirChildren(path string) string {
	return fmt.Sprintf(fsDirChildren, types.GeneratePathID(path))
}

func (rk *redisKeys) FsQueryResult(workspaceId uint, path string) string {
	return fmt.Sprintf(fsQueryResult, workspaceId, types.GeneratePathID(path))
}

func (rk *redisKeys) FsResultBody(workspaceId uint, path, resultId string) string {
	return fmt.Sprintf(fsResultBody, workspaceId, types.GeneratePathID(path), resultId)
}

// FsResultBodyIndex returns the set key that tracks all result-content cache
// keys for a workspace + query path.
func (rk *redisKeys) FsResultBodyIndex(workspaceId uint, path string) string {
	return fmt.Sprintf(fsResultIndex, workspaceId, types.GeneratePathID(path))
}

// FsCompressedIndex returns the set key that tracks all compressed cache keys
// (pointer + content) for a workspace + query path.
func (rk *redisKeys) FsCompressedIndex(workspaceId uint, path string) string {
	return fmt.Sprintf(fsCompIndex, workspaceId, types.GeneratePathID(path))
}

// --- Session keys ---

func (rk *redisKeys) SessionState(sessionId string) string {
	return fmt.Sprintf(sessionState, sessionId)
}

func (rk *redisKeys) SessionLock(sessionId string) string {
	return fmt.Sprintf(sessionLock, sessionId)
}

func (rk *redisKeys) SessionIndex() string {
	return sessionIndex
}

// --- Scheduler keys ---

func (rk *redisKeys) SchedulerWorkerLock(workerId string) string {
	return fmt.Sprintf(schedulerWorkerLock, workerId)
}

func (rk *redisKeys) SchedulerWorkerState(workerId string) string {
	return fmt.Sprintf(schedulerWorkerState, workerId)
}

func (rk *redisKeys) SchedulerWorkerIndex() string {
	return schedulerWorkerIndex
}

// --- Gateway keys ---

func (rk *redisKeys) GatewayInitLock(name string) string {
	return fmt.Sprintf(gatewayInitLock, name)
}

// --- Network keys ---

func (rk *redisKeys) NetworkIPLock() string {
	return networkIPLock
}

func (rk *redisKeys) NetworkIPPool() string {
	return networkIPPool
}

func (rk *redisKeys) NetworkIPMap() string {
	return networkIPMap
}

// --- Hook keys ---

func (rk *redisKeys) HookStream() string {
	return hookStream
}

func (rk *redisKeys) HookConsumerGroup() string {
	return hookConsumerGroup
}

func (rk *redisKeys) HookSeen(workspaceId uint, pathHash string) string {
	return fmt.Sprintf(hookSeen, workspaceId, pathHash)
}

func (rk *redisKeys) HookPollLock(queryExtId string) string {
	return fmt.Sprintf(hookPollLock, queryExtId)
}

// --- OAuth keys ---

func (rk *redisKeys) OAuthSession(sessionId string) string {
	return fmt.Sprintf(oauthSession, sessionId)
}

func (rk *redisKeys) OAuthState(state string) string {
	return fmt.Sprintf(oauthState, state)
}

// --- Run execution queue keys ---

func (rk *redisKeys) RunExecutionQueue(pool string) string {
	return fmt.Sprintf(runExecutionQueueKey, pool)
}

func (rk *redisKeys) RunExecutionInFlight(pool string) string {
	return fmt.Sprintf(runExecutionInFlightKey, pool)
}

func (rk *redisKeys) RunExecutionDelayed(pool string) string {
	return fmt.Sprintf(runExecutionDelayedKey, pool)
}

func (rk *redisKeys) RunExecutionState(runExecutionID string) string {
	return fmt.Sprintf(runExecutionStateKey, runExecutionID)
}

func (rk *redisKeys) RunExecutionResult(runExecutionID string) string {
	return fmt.Sprintf(runExecutionResultKey, runExecutionID)
}

func (rk *redisKeys) RunExecutionLogsChannel(runExecutionID string) string {
	return fmt.Sprintf(runExecutionLogsChannel, runExecutionID)
}

func (rk *redisKeys) RunExecutionLogsBuffer(runExecutionID string) string {
	return fmt.Sprintf(runExecutionLogsBuffer, runExecutionID)
}

// --- Orchestration stream keys ---

func (rk *redisKeys) OrchestrationTaskDispatchStream() string {
	return orchestrationTaskDispatchStream
}

func (rk *redisKeys) OrchestrationTaskDispatchGroup() string {
	return orchestrationTaskDispatchGroup
}

func (rk *redisKeys) OrchestrationTaskDispatchDLQ() string {
	return orchestrationTaskDispatchDLQ
}

func (rk *redisKeys) OrchestrationRunResultStream() string {
	return orchestrationRunResultStream
}

func (rk *redisKeys) OrchestrationRunResultGroup() string {
	return orchestrationRunResultGroup
}

func (rk *redisKeys) OrchestrationRunResultDLQ() string {
	return orchestrationRunResultDLQ
}

func (rk *redisKeys) AgentAttemptEvents() string {
	return agentAttemptEvents
}

func (rk *redisKeys) AgentRunEventsChannel(runID string) string {
	return fmt.Sprintf(agentRunEventsChannel, runID)
}

func (rk *redisKeys) AgentRunEventsBuffer(runID string) string {
	return fmt.Sprintf(agentRunEventsBuffer, runID)
}

func (rk *redisKeys) AgentRunRecoveryLock() string {
	return agentRunRecoveryLock
}

func (rk *redisKeys) AgentInstanceLock(instanceKey string) string {
	return fmt.Sprintf(agentInstanceLock, instanceKey)
}

// --- Terminal IO keys ---

func (rk *redisKeys) TerminalInput(taskId string) string {
	return fmt.Sprintf(terminalInput, taskId)
}

func (rk *redisKeys) TerminalOutput(taskId string) string {
	return fmt.Sprintf(terminalOutput, taskId)
}

func (rk *redisKeys) TerminalCancel(taskId string) string {
	return fmt.Sprintf(terminalCancel, taskId)
}

// --- Session lease keys ---

func (rk *redisKeys) SessionLease(workspaceId uint, sessionId string) string {
	return fmt.Sprintf(sessionLease, workspaceId, sessionId)
}

func (rk *redisKeys) SessionCheckpoint(workspaceId uint, sessionId string) string {
	return fmt.Sprintf(sessionCheckpoint, workspaceId, sessionId)
}

func (rk *redisKeys) RunInteraction(workspaceId uint, runId string) string {
	return fmt.Sprintf(runInteraction, workspaceId, runId)
}

// --- Compression keys ---

func (rk *redisKeys) FsCompressedPointer(workspaceId uint, path, resultId, strategy string) string {
	return fmt.Sprintf(fsCompressedPointer, workspaceId, types.GeneratePathID(path), resultId, strategy)
}

func (rk *redisKeys) FsCompressedContent(workspaceId uint, path, resultId, strategy string) string {
	return fmt.Sprintf(fsCompressedContent, workspaceId, types.GeneratePathID(path), resultId, strategy)
}

func (rk *redisKeys) FsCompressedUsage(workspaceId uint) string {
	return fmt.Sprintf(fsCompressedUsage, workspaceId)
}

// FsCompressedScanPatterns returns Redis SCAN patterns that match all
// compressed pointers, content, and usage keys for a workspace.
func (rk *redisKeys) FsCompressedScanPatterns(workspaceId uint) []string {
	return []string{
		fmt.Sprintf("airstore:compressed:%d:*", workspaceId),
		fmt.Sprintf("airstore:cc:%d:*", workspaceId),
		fmt.Sprintf("airstore:cc:usage:%d", workspaceId),
	}
}
