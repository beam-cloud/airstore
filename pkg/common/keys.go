package common

import "fmt"

var (
	// Filesystem metadata keys
	filesystemPrefix     string = "filesystem"
	filesystemDirAccess  string = "filesystem:dir:access:%s:%s" // pid, name
	filesystemDirContent string = "filesystem:dir:content:%s"   // id
	filesystemFile       string = "filesystem:file:%s:%s"       // pid, name

	// Session keys
	sessionPrefix string = "session"
	sessionState  string = "session:state:%s" // sessionId
	sessionLock   string = "session:lock:%s"  // sessionId
	sessionIndex  string = "session:index"

	// Scheduler keys
	schedulerPrefix      string = "scheduler"
	schedulerWorkerState string = "scheduler:worker:state:%s" // workerId
	schedulerWorkerLock  string = "scheduler:worker:lock:%s"  // workerId
	schedulerWorkerIndex string = "scheduler:worker:index"

	// Gateway keys
	gatewayPrefix   string = "gateway"
	gatewayInitLock string = "gateway:init:%s:lock" // name
)

var Keys = &redisKeys{}

type redisKeys struct{}

// Filesystem keys
func (rk *redisKeys) FilesystemPrefix() string {
	return filesystemPrefix
}

func (rk *redisKeys) FilesystemDirAccess(pid, name string) string {
	return fmt.Sprintf(filesystemDirAccess, pid, name)
}

func (rk *redisKeys) FilesystemDirContent(id string) string {
	return fmt.Sprintf(filesystemDirContent, id)
}

func (rk *redisKeys) FilesystemFile(pid, name string) string {
	return fmt.Sprintf(filesystemFile, pid, name)
}

// Session keys
func (rk *redisKeys) SessionPrefix() string {
	return sessionPrefix
}

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
func (rk *redisKeys) SchedulerPrefix() string {
	return schedulerPrefix
}

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
func (rk *redisKeys) GatewayPrefix() string {
	return gatewayPrefix
}

func (rk *redisKeys) GatewayInitLock(name string) string {
	return fmt.Sprintf(gatewayInitLock, name)
}
