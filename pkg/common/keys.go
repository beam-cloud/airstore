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
	taskQueueKey          = "airstore:task:queue"
	taskBacklogKey        = "airstore:task:backlog:%s"        // instanceKey
	taskModeStateKey      = "airstore:task:mode:%s"           // modeKey
	taskModeSetKey        = "airstore:task:mode:active"       // modeKey set
	agentDispatchLock     = "airstore:agent:dispatch:lock:%s" // instanceKey
	agentAttemptEvents    = "airstore:agent:attempt:events"
	agentRunEventsChannel = "airstore:agent:run:%s:events"
	agentRunEventsBuffer  = "airstore:agent:run:%s:events:buf"
	agentInstanceLock     = "airstore:agent:instance:lock:%s" // instanceKey

	// Terminal IO keys (pub/sub channels)
	terminalInput  = "airstore:terminal:%s:input"  // taskId
	terminalOutput = "airstore:terminal:%s:output" // taskId
	terminalCancel = "airstore:terminal:%s:cancel" // taskId

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

// --- Task queue keys ---

func (rk *redisKeys) TaskQueue() string {
	return taskQueueKey
}

func (rk *redisKeys) TaskBacklog(instanceKey string) string {
	return fmt.Sprintf(taskBacklogKey, instanceKey)
}

func (rk *redisKeys) TaskModeState(modeKey string) string {
	return fmt.Sprintf(taskModeStateKey, modeKey)
}

func (rk *redisKeys) TaskModeSet() string {
	return taskModeSetKey
}

func (rk *redisKeys) AgentDispatchLock(instanceKey string) string {
	return fmt.Sprintf(agentDispatchLock, instanceKey)
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
