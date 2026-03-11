package game

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/beam-cloud/airstore/pkg/company"
)

const maxActivityEvents = 40

type zoneBlueprint struct {
	ID       string
	Kind     company.ZoneKind
	Name     string
	Subtitle string
	Accent   string
	GridX    float64
	GridY    float64
	Width    int
	Height   int
}

var zoneBlueprints = []zoneBlueprint{
	{ID: string(company.ZoneKindCommandCenter), Kind: company.ZoneKindCommandCenter, Name: "Town Square", Subtitle: "Direct agents and review the city's pulse", Accent: "#1565c0", GridX: 0, GridY: 0, Width: 5, Height: 4},
	{ID: string(company.ZoneKindActiveOps), Kind: company.ZoneKindActiveOps, Name: "Operations Ward", Subtitle: "Live work, casts, and active quests", Accent: "#00b894", GridX: 6, GridY: 0, Width: 5, Height: 4},
	{ID: string(company.ZoneKindAttentionTower), Kind: company.ZoneKindAttentionTower, Name: "Watchtower", Subtitle: "Inbox alerts, blockers, and errors", Accent: "#c62828", GridX: 12, GridY: 0, Width: 4, Height: 4},
	{ID: string(company.ZoneKindSourceDistrict), Kind: company.ZoneKindSourceDistrict, Name: "Source Bazaar", Subtitle: "Connected systems, tools, and vendors", Accent: "#dc8b4f", GridX: 0, GridY: 5, Width: 4, Height: 3},
	{ID: string(company.ZoneKindSchedulingHall), Kind: company.ZoneKindSchedulingHall, Name: "Clockwork Hall", Subtitle: "Timers, wakes, and queued follow-ups", Accent: "#7c5cbf", GridX: 5, GridY: 5, Width: 5, Height: 3},
	{ID: string(company.ZoneKindResultsArchive), Kind: company.ZoneKindResultsArchive, Name: "Postmaster Keep", Subtitle: "Mail, outputs, and finished work", Accent: "#0f8f8f", GridX: 11, GridY: 5, Width: 5, Height: 3},
}

func ProjectSnapshot(workspaceID string, version int64, business *company.CompanySnapshot, existingActivity []company.ActivityFeedEvent) (*company.CompanyWorldSnapshot, []company.ActivityFeedEvent) {
	if business == nil {
		business = &company.CompanySnapshot{}
	}

	zoneStats := make(map[string]*company.ZoneSummary, len(zoneBlueprints))
	zones := make([]company.ZoneSummary, 0, len(zoneBlueprints))
	for _, bp := range zoneBlueprints {
		zone := company.ZoneSummary{
			ID:       bp.ID,
			Kind:     bp.Kind,
			Name:     bp.Name,
			Subtitle: bp.Subtitle,
			Accent:   bp.Accent,
			GridX:    bp.GridX,
			GridY:    bp.GridY,
			Width:    bp.Width,
			Height:   bp.Height,
		}
		zones = append(zones, zone)
		zoneStats[bp.ID] = &zones[len(zones)-1]
	}

	taskIndexByAgent := map[string][]company.TaskSummary{}
	for _, task := range business.RunningTasks {
		if task.AgentID == "" {
			continue
		}
		taskIndexByAgent[task.AgentID] = append(taskIndexByAgent[task.AgentID], task)
	}

	entities := make([]company.EntitySummary, 0, len(business.Agents)+len(business.Sources)+len(business.RecentResults)+len(business.ScheduledTasks))
	zoneEntityCount := map[string]int{}

	// Agents are the main world entities/NPCs.
	agents := append([]company.AgentSummary(nil), business.Agents...)
	sort.SliceStable(agents, func(i, j int) bool {
		if agents[i].State == agents[j].State {
			return agents[i].Name < agents[j].Name
		}
		return agents[i].State < agents[j].State
	})

	for _, agent := range agents {
		zoneID := zoneForAgentState(string(agent.State))
		slot := zoneEntityCount[zoneID]
		pos := zoneSlotPosition(zoneID, slot)
		zoneEntityCount[zoneID]++
		zone := zoneStats[zoneID]
		zone.EntityCount++
		zone.TaskCount += agent.ActiveTaskCount
		if agent.State == company.AgentDerivedStateWorking {
			zone.ActiveCount++
		}

		agentTasks := taskIndexByAgent[agent.ID]
		castLabel := ""
		if len(agentTasks) > 0 {
			castLabel = trimLabel(agentTasks[0].PromptSummary, 32)
		}

		entities = append(entities, company.EntitySummary{
			ID:         agent.ID,
			Kind:       company.EntityKindAgent,
			ZoneID:     zoneID,
			Name:       agent.Name,
			Subtitle:   agent.Role,
			Accent:     accentForAgentState(string(agent.State)),
			State:      string(agent.State),
			Animation:  animationForAgentState(string(agent.State)),
			GridX:      pos.X,
			GridY:      pos.Y,
			Facing:     facingForSlot(slot),
			TaskCount:  agent.ActiveTaskCount,
			CostUSD:    agent.TotalCostUSD,
			Skills:     agent.Skills,
			StatusText: statusTextForAgent(agent),
			Health:     healthForAgentState(string(agent.State)),
			Mana:       manaForAgent(agent),
			CastLabel:  castLabel,
			Level:      levelForAgent(agent),
			Badges:     badgesForAgent(agent),
			Model:      agent.Model,
		})
	}

	// Sources appear as props/buildings.
	for i, source := range business.Sources {
		zoneID := string(company.ZoneKindSourceDistrict)
		pos := zoneSlotPosition(zoneID, zoneEntityCount[zoneID]+i)
		zone := zoneStats[zoneID]
		zone.EntityCount++
		entities = append(entities, company.EntitySummary{
			ID:         fmt.Sprintf("source:%s:%d", source.IntegrationType, i),
			Kind:       company.EntityKindSource,
			ZoneID:     zoneID,
			Name:       strings.Title(source.IntegrationType),
			Subtitle:   "Source",
			Accent:     accentForSourceStatus(source.Status),
			State:      source.Status,
			Animation:  animationForSourceStatus(source.Status),
			GridX:      pos.X,
			GridY:      pos.Y,
			TaskCount:  0,
			CostUSD:    0,
			StatusText: strings.Title(source.Status),
			Health:     1,
			Mana:       0.6,
			Level:      1,
		})
	}

	// Recent results become archive props.
	for i, result := range business.RecentResults {
		zoneID := string(company.ZoneKindResultsArchive)
		pos := zoneSlotPosition(zoneID, zoneEntityCount[zoneID]+i)
		zone := zoneStats[zoneID]
		zone.EntityCount++
		entities = append(entities, company.EntitySummary{
			ID:         fmt.Sprintf("result:%s", result.ID),
			Kind:       company.EntityKindResult,
			ZoneID:     zoneID,
			Name:       trimLabel(result.Prompt, 28),
			Subtitle:   result.AgentName,
			Accent:     accentForResultState(result.State),
			State:      result.State,
			Animation:  company.EntityAnimationStateCelebrate,
			GridX:      pos.X,
			GridY:      pos.Y,
			CostUSD:    result.CostUSD,
			StatusText: "Completed",
			Health:     1,
			Mana:       1,
			Level:      1,
			Badges:     []string{"result"},
		})
	}

	// Schedule pylons.
	for i, schedule := range business.ScheduledTasks {
		zoneID := string(company.ZoneKindSchedulingHall)
		pos := zoneSlotPosition(zoneID, zoneEntityCount[zoneID]+i)
		zone := zoneStats[zoneID]
		zone.EntityCount++
		entities = append(entities, company.EntitySummary{
			ID:         fmt.Sprintf("schedule:%s", schedule.ID),
			Kind:       company.EntityKindSchedule,
			ZoneID:     zoneID,
			Name:       trimLabel(schedule.Prompt, 28),
			Subtitle:   schedule.AgentName,
			Accent:     accentForSchedule(schedule.Active),
			State:      ternaryState(schedule.Active, "active", "paused"),
			Animation:  ternaryAnimation(schedule.Active, company.EntityAnimationStateCasting, company.EntityAnimationStateSleeping),
			GridX:      pos.X,
			GridY:      pos.Y,
			StatusText: schedule.CronExpr,
			Health:     1,
			Mana:       0.75,
			Level:      1,
			Badges:     []string{"schedule"},
		})
	}

	taskBeacons := buildTaskBeacons(business, entities)

	snapshot := &company.CompanyWorldSnapshot{
		WorkspaceID: workspaceID,
		Version:     version,
		GeneratedAt: nowMs(),
		Camera: company.WorldCameraPreset{
			Mode:   "isometric",
			Center: company.WorldVec2{X: 8, Y: 6},
			Zoom:   1,
		},
		Zones:       zones,
		Entities:    entities,
		TaskBeacons: taskBeacons,
		Activity:    existingActivity,
		Hud: company.WorldHudSummary{
			AgentCount:      len(business.Agents),
			ActiveTaskCount: len(business.RunningTasks),
			ScheduleCount:   len(business.ScheduledTasks),
			SourceCount:     len(business.Sources),
			TotalSpend:      business.CostSummary.TotalUSD,
			Connected:       true,
			RuntimeVersion:  version,
			Tick:            nowMs(),
		},
	}

	return snapshot, snapshot.Activity
}

func DiffWorld(previous, next *company.CompanyWorldSnapshot, seq int64, activity []company.ActivityFeedEvent) *company.CompanyWorldDelta {
	if next == nil {
		return nil
	}
	delta := &company.CompanyWorldDelta{
		Sequence:    seq,
		GeneratedAt: next.GeneratedAt,
		TaskBeacons: next.TaskBeacons,
		Hud:         &next.Hud,
		Activity:    activity,
		Camera:      &next.Camera,
	}
	if previous == nil {
		delta.UpdatedZones = next.Zones
		delta.UpdatedEntities = next.Entities
		return delta
	}

	prevZones := make(map[string]company.ZoneSummary, len(previous.Zones))
	for _, zone := range previous.Zones {
		prevZones[zone.ID] = zone
	}
	for _, zone := range next.Zones {
		if prev, ok := prevZones[zone.ID]; !ok || !reflect.DeepEqual(prev, zone) {
			delta.UpdatedZones = append(delta.UpdatedZones, zone)
		}
	}

	prevEntities := make(map[string]company.EntitySummary, len(previous.Entities))
	for _, entity := range previous.Entities {
		prevEntities[entity.ID] = entity
	}
	nextEntities := make(map[string]company.EntitySummary, len(next.Entities))
	for _, entity := range next.Entities {
		nextEntities[entity.ID] = entity
		if prev, ok := prevEntities[entity.ID]; !ok || !reflect.DeepEqual(prev, entity) {
			delta.UpdatedEntities = append(delta.UpdatedEntities, entity)
		}
	}
	for id := range prevEntities {
		if _, ok := nextEntities[id]; !ok {
			delta.RemovedEntityIDs = append(delta.RemovedEntityIDs, id)
		}
	}

	return delta
}

func BuildActivityFeed(previous, next *company.CompanySnapshot, existing []company.ActivityFeedEvent) ([]company.ActivityFeedEvent, []company.ActivityFeedEvent) {
	if next == nil {
		return existing, nil
	}
	feed := append([]company.ActivityFeedEvent(nil), existing...)
	newEvents := make([]company.ActivityFeedEvent, 0, 8)

	if previous == nil {
		if len(next.Agents) > 0 {
			evt := company.ActivityFeedEvent{
				ID:        fmt.Sprintf("bootstrap:%d", nowMs()),
				Kind:      "world",
				Channel:   "system",
				Message:   fmt.Sprintf("World synced with %d agent(s) online.", len(next.Agents)),
				Timestamp: nowMs(),
			}
			feed = append(feed, evt)
			newEvents = append(newEvents, evt)
		}
		return trimActivity(feed), newEvents
	}

	prevAgents := map[string]company.AgentSummary{}
	for _, agent := range previous.Agents {
		prevAgents[agent.ID] = agent
	}
	for _, agent := range next.Agents {
		if prev, ok := prevAgents[agent.ID]; ok && prev.State != agent.State {
			evt := company.ActivityFeedEvent{
				ID:        fmt.Sprintf("agent:%s:%d", agent.ID, nowMs()),
				Kind:      "agent_state",
				Channel:   channelForAgentState(string(agent.State)),
				EntityID:  agent.ID,
				Message:   fmt.Sprintf("%s moved from %s to %s.", agent.Name, strings.ReplaceAll(string(prev.State), "_", " "), strings.ReplaceAll(string(agent.State), "_", " ")),
				Timestamp: nowMs(),
			}
			feed = append(feed, evt)
			newEvents = append(newEvents, evt)
		}
	}

	prevTasks := map[string]company.TaskSummary{}
	for _, task := range previous.RunningTasks {
		prevTasks[task.ID] = task
	}
	for _, task := range next.RunningTasks {
		if _, ok := prevTasks[task.ID]; !ok {
			evt := company.ActivityFeedEvent{
				ID:        fmt.Sprintf("task:%s:%d", task.ID, nowMs()),
				Kind:      "task_started",
				Channel:   "combat",
				EntityID:  task.AgentID,
				Message:   fmt.Sprintf("%s picked up: %s", fallback(task.AgentName, "An agent"), trimLabel(task.PromptSummary, 64)),
				Timestamp: nowMs(),
			}
			feed = append(feed, evt)
			newEvents = append(newEvents, evt)
		}
	}

	prevResults := map[string]company.TaskResultSummary{}
	for _, result := range previous.RecentResults {
		prevResults[result.ID] = result
	}
	for _, result := range next.RecentResults {
		if _, ok := prevResults[result.ID]; !ok {
			evt := company.ActivityFeedEvent{
				ID:        fmt.Sprintf("result:%s:%d", result.ID, nowMs()),
				Kind:      "task_finished",
				Channel:   "loot",
				EntityID:  result.AgentID,
				Message:   fmt.Sprintf("%s completed: %s", fallback(result.AgentName, "An agent"), trimLabel(result.Prompt, 64)),
				Timestamp: nowMs(),
			}
			feed = append(feed, evt)
			newEvents = append(newEvents, evt)
		}
	}

	prevSources := map[string]string{}
	for _, source := range previous.Sources {
		prevSources[source.IntegrationType] = source.Status
	}
	for _, source := range next.Sources {
		if prevStatus, ok := prevSources[source.IntegrationType]; ok && prevStatus != source.Status {
			evt := company.ActivityFeedEvent{
				ID:        fmt.Sprintf("source:%s:%d", source.IntegrationType, nowMs()),
				Kind:      "source_changed",
				Channel:   "system",
				Message:   fmt.Sprintf("%s is now %s.", strings.Title(source.IntegrationType), source.Status),
				Timestamp: nowMs(),
			}
			feed = append(feed, evt)
			newEvents = append(newEvents, evt)
		}
	}

	return trimActivity(feed), newEvents
}

func buildTaskBeacons(business *company.CompanySnapshot, entities []company.EntitySummary) []company.TaskBeaconSummary {
	entityByID := make(map[string]company.EntitySummary, len(entities))
	for _, entity := range entities {
		entityByID[entity.ID] = entity
	}
	out := make([]company.TaskBeaconSummary, 0, len(business.RunningTasks))
	perAgent := map[string]int{}
	for _, task := range business.RunningTasks {
		zoneID := string(company.ZoneKindActiveOps)
		gridX := 11.0
		gridY := 2.0
		if entity, ok := entityByID[task.AgentID]; ok {
			zoneID = entity.ZoneID
			index := perAgent[task.AgentID]
			perAgent[task.AgentID]++
			gridX = entity.GridX + 0.8 + float64(index%2)*0.6
			gridY = entity.GridY - 0.4 + float64(index/2)*0.5
		}
		progress := 0.2
		if task.DurationSec > 0 {
			progress = 0.35 + float64(task.DurationSec%45)/90
		}
		if progress > 0.95 {
			progress = 0.95
		}
		out = append(out, company.TaskBeaconSummary{
			ID:          task.ID,
			ZoneID:      zoneID,
			AgentID:     task.AgentID,
			Label:       trimLabel(task.PromptSummary, 26),
			State:       task.State,
			Priority:    task.Priority,
			GridX:       gridX,
			GridY:       gridY,
			Progress:    progress,
			DurationSec: task.DurationSec,
			CreatedAt:   task.CreatedAt,
		})
	}
	return out
}

func zoneForAgentState(state string) string {
	switch state {
	case "working":
		return string(company.ZoneKindActiveOps)
	case "waiting", "sleeping":
		return string(company.ZoneKindSchedulingHall)
	case "error":
		return string(company.ZoneKindAttentionTower)
	default:
		return string(company.ZoneKindCommandCenter)
	}
}

func zoneSlotPosition(zoneID string, slot int) company.WorldVec2 {
	bp := lookupZone(zoneID)
	if bp == nil {
		return company.WorldVec2{}
	}
	innerWidth := max(bp.Width-2, 1)
	row := slot / innerWidth
	col := slot % innerWidth
	if row >= max(bp.Height-2, 1) {
		row = row % max(bp.Height-2, 1)
	}
	return company.WorldVec2{
		X: bp.GridX + 1 + float64(col),
		Y: bp.GridY + 1 + float64(row),
	}
}

func lookupZone(zoneID string) *zoneBlueprint {
	for i := range zoneBlueprints {
		if zoneBlueprints[i].ID == zoneID {
			return &zoneBlueprints[i]
		}
	}
	return nil
}

func accentForAgentState(state string) string {
	switch state {
	case "working":
		return "#00b894"
	case "waiting":
		return "#dc8b4f"
	case "sleeping":
		return "#7c5cbf"
	case "error":
		return "#c62828"
	default:
		return "#1565c0"
	}
}

func animationForAgentState(state string) company.EntityAnimationState {
	switch state {
	case "working":
		return company.EntityAnimationStateCasting
	case "waiting":
		return company.EntityAnimationStateWaiting
	case "sleeping":
		return company.EntityAnimationStateSleeping
	case "error":
		return company.EntityAnimationStateError
	default:
		return company.EntityAnimationStateIdle
	}
}

func statusTextForAgent(agent company.AgentSummary) string {
	switch agent.State {
	case "working":
		return fmt.Sprintf("%d quest(s) in progress", agent.ActiveTaskCount)
	case "waiting":
		return "Awaiting input"
	case "sleeping":
		return "Resting until wake"
	case "error":
		return "Needs intervention"
	default:
		return "Ready for orders"
	}
}

func healthForAgentState(state string) float64 {
	switch state {
	case "error":
		return 0.35
	case "sleeping":
		return 0.7
	default:
		return 1
	}
}

func manaForAgent(agent company.AgentSummary) float64 {
	if agent.ActiveTaskCount <= 0 {
		return 0.55
	}
	if agent.ActiveTaskCount >= 4 {
		return 0.95
	}
	return 0.6 + float64(agent.ActiveTaskCount)*0.08
}

func levelForAgent(agent company.AgentSummary) int {
	level := 1 + len(agent.Skills)
	if agent.Model != "" {
		level++
	}
	if level > 12 {
		level = 12
	}
	return level
}

func badgesForAgent(agent company.AgentSummary) []string {
	badges := make([]string, 0, 3)
	if !agent.Active {
		badges = append(badges, "offline")
	}
	if agent.ActiveTaskCount > 0 {
		badges = append(badges, "busy")
	}
	if len(agent.Skills) > 0 {
		badges = append(badges, fmt.Sprintf("%d skills", len(agent.Skills)))
	}
	return badges
}

func accentForSourceStatus(status string) string {
	if status == "expired" {
		return "#c62828"
	}
	return "#dc8b4f"
}

func animationForSourceStatus(status string) company.EntityAnimationState {
	if status == "expired" {
		return company.EntityAnimationStateError
	}
	return company.EntityAnimationStateWorking
}

func accentForResultState(state string) string {
	if strings.Contains(strings.ToLower(state), "error") {
		return "#c62828"
	}
	return "#0f8f8f"
}

func accentForSchedule(active bool) string {
	if active {
		return "#7c5cbf"
	}
	return "#6b7280"
}

func channelForAgentState(state string) string {
	switch state {
	case "working":
		return "raid"
	case "error":
		return "warning"
	default:
		return "system"
	}
}

func facingForSlot(slot int) string {
	if slot%2 == 0 {
		return "right"
	}
	return "left"
}

func trimActivity(feed []company.ActivityFeedEvent) []company.ActivityFeedEvent {
	if len(feed) <= maxActivityEvents {
		return feed
	}
	return append([]company.ActivityFeedEvent(nil), feed[len(feed)-maxActivityEvents:]...)
}

func trimLabel(value string, maxLen int) string {
	value = strings.TrimSpace(value)
	if len(value) <= maxLen {
		return value
	}
	if maxLen <= 3 {
		return value[:maxLen]
	}
	return value[:maxLen-3] + "..."
}

func fallback(primary, alt string) string {
	if strings.TrimSpace(primary) == "" {
		return alt
	}
	return primary
}

func ternaryState(active bool, on, off string) string {
	if active {
		return on
	}
	return off
}

func ternaryAnimation(active bool, on, off company.EntityAnimationState) company.EntityAnimationState {
	if active {
		return on
	}
	return off
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
