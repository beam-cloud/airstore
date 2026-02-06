package common

import (
	"fmt"

	"github.com/beam-cloud/airstore/pkg/types"
)

var (
	// Filesystem VNode metadata keys (used by FUSE layer)
	filesystemDirAccess  string = "filesystem:dir:access:%s:%s" // pid, name
	filesystemDirContent string = "filesystem:dir:content:%s"   // id
	filesystemFile       string = "filesystem:file:%s:%s"       // pid, name

	// Filesystem store cache keys (used by FilesystemStore)
	fsDirMeta     string = "airstore:fs:dir:%s"   // pathHash
	fsFileMeta    string = "airstore:fs:file:%s"  // pathHash
	fsSymlink     string = "airstore:fs:link:%s"  // pathHash
	fsDirChildren string = "airstore:fs:ls:%s"    // pathHash
	fsQueryResult string = "airstore:qr:%d:%s"    // workspaceId, pathHash
	fsResultBody  string = "airstore:rc:%d:%s:%s" // workspaceId, pathHash, resultId

	// Session keys
	sessionState string = "session:state:%s" // sessionId
	sessionLock  string = "session:lock:%s"  // sessionId
	sessionIndex string = "session:index"

	// Scheduler keys
	schedulerWorkerState string = "scheduler:worker:state:%s" // workerId
	schedulerWorkerLock  string = "scheduler:worker:lock:%s"  // workerId
	schedulerWorkerIndex string = "scheduler:worker:index"

	// Gateway keys
	gatewayInitLock string = "gateway:init:%s:lock" // name

	// Network keys
	networkIPLock string = "network:ip:lock"
	networkIPPool string = "network:pool"
	networkIPMap  string = "network:mapping"

	// Hook keys
	hookStream        string = "hook:events"
	hookConsumerGroup string = "hook-evaluators"
	hookSeen          string = "hook:seen:%d:%s" // workspaceId, pathHash
	hookPollLock      string = "hook:poll:%s"    // queryExternalId
)

var Keys = &redisKeys{}

type redisKeys struct{}

// Filesystem keys

func (rk *redisKeys) FilesystemDirAccess(pid, name string) string {
	return fmt.Sprintf(filesystemDirAccess, pid, name)
}

func (rk *redisKeys) FilesystemDirContent(id string) string {
	return fmt.Sprintf(filesystemDirContent, id)
}

func (rk *redisKeys) FilesystemFile(pid, name string) string {
	return fmt.Sprintf(filesystemFile, pid, name)
}

// Filesystem store cache keys (path args are auto-hashed)

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

// Session keys

func (rk *redisKeys) SessionState(sessionId string) string {
	return fmt.Sprintf(sessionState, sessionId)
}

func (rk *redisKeys) SessionLock(sessionId string) string {
	return fmt.Sprintf(sessionLock, sessionId)
}

func (rk *redisKeys) SessionIndex() string {
	return sessionIndex
}

// Scheduler keys

func (rk *redisKeys) SchedulerWorkerLock(workerId string) string {
	return fmt.Sprintf(schedulerWorkerLock, workerId)
}

func (rk *redisKeys) SchedulerWorkerState(workerId string) string {
	return fmt.Sprintf(schedulerWorkerState, workerId)
}

func (rk *redisKeys) SchedulerWorkerIndex() string {
	return schedulerWorkerIndex
}

// Gateway keys

func (rk *redisKeys) GatewayInitLock(name string) string {
	return fmt.Sprintf(gatewayInitLock, name)
}

// Network keys

func (rk *redisKeys) NetworkIPLock() string {
	return networkIPLock
}

func (rk *redisKeys) NetworkIPPool() string {
	return networkIPPool
}

func (rk *redisKeys) NetworkIPMap() string {
	return networkIPMap
}

// Hook keys

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
