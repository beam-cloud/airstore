package scheduler

import (
	"net/http"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
)

// SchedulerService provides HTTP endpoints for scheduler operations
type SchedulerService struct {
	scheduler *Scheduler
}

// NewSchedulerService creates a new SchedulerService
func NewSchedulerService(scheduler *Scheduler) *SchedulerService {
	return &SchedulerService{
		scheduler: scheduler,
	}
}

// RegisterRoutes registers the scheduler HTTP routes
func (s *SchedulerService) RegisterRoutes(g *echo.Group) {
	// Worker endpoints
	g.POST("/workers/register", s.RegisterWorker)
	g.DELETE("/workers/:id", s.DeregisterWorker)
	g.POST("/workers/:id/heartbeat", s.WorkerHeartbeat)
	g.PUT("/workers/:id/status", s.UpdateWorkerStatus)
	g.GET("/workers", s.ListWorkers)
	g.GET("/workers/:id", s.GetWorker)
}

// RegisterWorkerRequest is the request body for registering a worker
type RegisterWorkerRequest struct {
	Hostname string `json:"hostname"`
	PoolName string `json:"pool_name"`
	Cpu      int64  `json:"cpu"`
	Memory   int64  `json:"memory"`
	Version  string `json:"version"`
}

// RegisterWorkerResponse is the response for registering a worker
type RegisterWorkerResponse struct {
	WorkerID string `json:"worker_id"`
}

// RegisterWorker handles POST /workers/register
func (s *SchedulerService) RegisterWorker(c echo.Context) error {
	var req RegisterWorkerRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request body"})
	}

	worker := &types.Worker{
		Hostname: req.Hostname,
		PoolName: req.PoolName,
		Cpu:      req.Cpu,
		Memory:   req.Memory,
		Version:  req.Version,
	}

	if err := s.scheduler.RegisterWorker(c.Request().Context(), worker); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusCreated, RegisterWorkerResponse{WorkerID: worker.ID})
}

// DeregisterWorker handles DELETE /workers/:id
func (s *SchedulerService) DeregisterWorker(c echo.Context) error {
	workerId := c.Param("id")

	if err := s.scheduler.DeregisterWorker(c.Request().Context(), workerId); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"status": "deregistered"})
}

// WorkerHeartbeat handles POST /workers/:id/heartbeat
func (s *SchedulerService) WorkerHeartbeat(c echo.Context) error {
	workerId := c.Param("id")

	if err := s.scheduler.WorkerHeartbeat(c.Request().Context(), workerId); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
}

// UpdateWorkerStatusRequest is the request body for updating worker status
type UpdateWorkerStatusRequest struct {
	Status types.WorkerStatus `json:"status"`
}

// UpdateWorkerStatus handles PUT /workers/:id/status
func (s *SchedulerService) UpdateWorkerStatus(c echo.Context) error {
	workerId := c.Param("id")

	var req UpdateWorkerStatusRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request body"})
	}

	if err := s.scheduler.UpdateWorkerStatus(c.Request().Context(), workerId, req.Status); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"status": "updated"})
}

// ListWorkers handles GET /workers
func (s *SchedulerService) ListWorkers(c echo.Context) error {
	workers, err := s.scheduler.GetWorkers(c.Request().Context())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"workers": workers,
		"count":   len(workers),
	})
}

// GetWorker handles GET /workers/:id
func (s *SchedulerService) GetWorker(c echo.Context) error {
	workerId := c.Param("id")

	worker, err := s.scheduler.GetWorker(c.Request().Context(), workerId)
	if err != nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "worker not found"})
	}

	return c.JSON(http.StatusOK, worker)
}
