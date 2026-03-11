package game

import (
	"math"

	"github.com/beam-cloud/airstore/pkg/company"
)

// ---------------------------------------------------------------------------
// World Map — backend-authoritative world definition ("WAD")
// ---------------------------------------------------------------------------

type WorldVec3 struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

type WorldMap struct {
	Terrain     TerrainConfig    `json:"terrain"`
	Zones       []ZoneDefinition `json:"zones"`
	Decorations []DecorationDef  `json:"decorations"`
	EntityTypes []EntityTypeDef  `json:"entity_types"`
	Spawn       WorldVec3        `json:"spawn"`
}

type TerrainConfig struct {
	GroundSize  float64 `json:"ground_size"`
	GridDivs    int     `json:"grid_divs"`
	GroundColor string  `json:"ground_color"`
	GridColor1  string  `json:"grid_color_1"`
	GridColor2  string  `json:"grid_color_2"`
	FogNear     float64 `json:"fog_near"`
	FogFar      float64 `json:"fog_far"`
	FogColor    string  `json:"fog_color"`
}

type CubeDef struct {
	Offset     WorldVec3 `json:"offset"`
	Size       float64   `json:"size"`
	FloatSpeed float64   `json:"float_speed"`
	RotSpeed   float64   `json:"rot_speed"`
}

type ZoneDefinition struct {
	Kind        company.ZoneKind `json:"kind"`
	Name        string           `json:"name"`
	Subtitle    string           `json:"subtitle"`
	Accent      string           `json:"accent"`
	Position    WorldVec3        `json:"position"`
	Cubes       []CubeDef        `json:"cubes"`
	LabelY      float64          `json:"label_y"`
	EntitySlots []WorldVec3      `json:"entity_slots"`
}

type DecorationDef struct {
	Position WorldVec3 `json:"position"`
	BaseSize float64   `json:"base_size"`
	Steps    int       `json:"steps"`
	Shrink   float64   `json:"shrink"`
	Jitter   float64   `json:"jitter"`
}

type EntityTypeDef struct {
	Kind   company.EntityKind `json:"kind"`
	Shape  string             `json:"shape"` // "cube" | "octahedron"
	Size   float64            `json:"size"`
	FloatY float64            `json:"float_y"`
}

// ---------------------------------------------------------------------------
// Default world layout
// ---------------------------------------------------------------------------

func DefaultWorldMap() *WorldMap {
	return &WorldMap{
		Terrain: TerrainConfig{
			GroundSize:  200,
			GridDivs:    80,
			GroundColor: "#e4e8f0",
			GridColor1:  "#b8c4d8",
			GridColor2:  "#c8d0e0",
			FogNear:     30,
			FogFar:      120,
			FogColor:    "#e8ecf4",
		},
		Spawn: WorldVec3{X: 0, Y: 0.6, Z: 35},
		Zones: defaultZones(),
		Decorations: defaultDecorations(),
		EntityTypes: []EntityTypeDef{
			{Kind: company.EntityKindAgent, Shape: "cube", Size: 0.5, FloatY: 1.4},
			{Kind: company.EntityKindSource, Shape: "octahedron", Size: 0.35, FloatY: 1.0},
			{Kind: company.EntityKindResult, Shape: "octahedron", Size: 0.35, FloatY: 1.0},
			{Kind: company.EntityKindSchedule, Shape: "octahedron", Size: 0.35, FloatY: 1.0},
		},
	}
}

func defaultZones() []ZoneDefinition {
	return []ZoneDefinition{
		{
			Kind:     company.ZoneKindCommandCenter,
			Name:     "Town Square",
			Subtitle: "Direct agents and review the city's pulse",
			Accent:   "#1565c0",
			Position: WorldVec3{X: 0, Y: 0, Z: 0},
			LabelY:   11,
			Cubes: []CubeDef{
				{Offset: WorldVec3{X: 0, Y: 5, Z: 0}, Size: 2.2, FloatSpeed: 0.4, RotSpeed: 0.15},
				{Offset: WorldVec3{X: -2.5, Y: 3, Z: 1.5}, Size: 0.9, FloatSpeed: 0.6, RotSpeed: 0.3},
				{Offset: WorldVec3{X: 2.2, Y: 3.5, Z: -1.8}, Size: 1.0, FloatSpeed: 0.55, RotSpeed: 0.25},
				{Offset: WorldVec3{X: 0.5, Y: 7.5, Z: 1}, Size: 0.7, FloatSpeed: 0.7, RotSpeed: 0.35},
				{Offset: WorldVec3{X: -1.5, Y: 8, Z: -1}, Size: 0.6, FloatSpeed: 0.65, RotSpeed: 0.4},
			},
			EntitySlots: generateSlots(WorldVec3{X: 0, Y: 0, Z: 0}, 7, 12),
		},
		{
			Kind:     company.ZoneKindActiveOps,
			Name:     "Operations Ward",
			Subtitle: "Live work, casts, and active quests",
			Accent:   "#00b894",
			Position: WorldVec3{X: 28, Y: 0, Z: -5},
			LabelY:   7.5,
			Cubes: []CubeDef{
				{Offset: WorldVec3{X: -2.5, Y: 3, Z: 0}, Size: 1.2, FloatSpeed: 0.5, RotSpeed: 0.2},
				{Offset: WorldVec3{X: 0, Y: 3.5, Z: 0}, Size: 1.3, FloatSpeed: 0.45, RotSpeed: 0.18},
				{Offset: WorldVec3{X: 2.5, Y: 3, Z: 0}, Size: 1.1, FloatSpeed: 0.55, RotSpeed: 0.22},
				{Offset: WorldVec3{X: -1.2, Y: 5.5, Z: 1}, Size: 0.7, FloatSpeed: 0.65, RotSpeed: 0.3},
				{Offset: WorldVec3{X: 1.3, Y: 5.8, Z: -0.8}, Size: 0.65, FloatSpeed: 0.6, RotSpeed: 0.35},
				{Offset: WorldVec3{X: 0, Y: 4.5, Z: -1.5}, Size: 0.8, FloatSpeed: 0.5, RotSpeed: 0.28},
			},
			EntitySlots: generateSlots(WorldVec3{X: 28, Y: 0, Z: -5}, 7, 12),
		},
		{
			Kind:     company.ZoneKindAttentionTower,
			Name:     "Watchtower",
			Subtitle: "Inbox alerts, blockers, and errors",
			Accent:   "#c62828",
			Position: WorldVec3{X: -24, Y: 0, Z: 8},
			LabelY:   14,
			Cubes: []CubeDef{
				{Offset: WorldVec3{X: 0, Y: 3, Z: 0}, Size: 1.6, FloatSpeed: 0.4, RotSpeed: 0.15},
				{Offset: WorldVec3{X: 0.3, Y: 6.5, Z: -0.3}, Size: 1.2, FloatSpeed: 0.5, RotSpeed: 0.2},
				{Offset: WorldVec3{X: -0.2, Y: 10, Z: 0.2}, Size: 0.8, FloatSpeed: 0.6, RotSpeed: 0.3},
			},
			EntitySlots: generateSlots(WorldVec3{X: -24, Y: 0, Z: 8}, 7, 8),
		},
		{
			Kind:     company.ZoneKindSourceDistrict,
			Name:     "Source Bazaar",
			Subtitle: "Connected systems, tools, and vendors",
			Accent:   "#dc8b4f",
			Position: WorldVec3{X: 16, Y: 0, Z: 24},
			LabelY:   7,
			Cubes: []CubeDef{
				{Offset: WorldVec3{X: -2, Y: 2.5, Z: -1.5}, Size: 0.7, FloatSpeed: 0.6, RotSpeed: 0.3},
				{Offset: WorldVec3{X: 1.5, Y: 3, Z: 1}, Size: 0.8, FloatSpeed: 0.5, RotSpeed: 0.25},
				{Offset: WorldVec3{X: -1, Y: 3.5, Z: 2}, Size: 0.6, FloatSpeed: 0.7, RotSpeed: 0.35},
				{Offset: WorldVec3{X: 2.5, Y: 2, Z: -0.5}, Size: 0.65, FloatSpeed: 0.55, RotSpeed: 0.28},
				{Offset: WorldVec3{X: 0, Y: 4, Z: 0}, Size: 1.0, FloatSpeed: 0.45, RotSpeed: 0.2},
				{Offset: WorldVec3{X: -2.5, Y: 4.5, Z: 0.5}, Size: 0.5, FloatSpeed: 0.75, RotSpeed: 0.4},
				{Offset: WorldVec3{X: 1, Y: 5, Z: -2}, Size: 0.55, FloatSpeed: 0.65, RotSpeed: 0.32},
				{Offset: WorldVec3{X: 0.5, Y: 2, Z: 2.5}, Size: 0.6, FloatSpeed: 0.6, RotSpeed: 0.3},
			},
			EntitySlots: generateSlots(WorldVec3{X: 16, Y: 0, Z: 24}, 7, 12),
		},
		{
			Kind:     company.ZoneKindSchedulingHall,
			Name:     "Clockwork Hall",
			Subtitle: "Timers, wakes, and queued follow-ups",
			Accent:   "#7c5cbf",
			Position: WorldVec3{X: -16, Y: 0, Z: -22},
			LabelY:   8.5,
			Cubes: []CubeDef{
				{Offset: WorldVec3{X: -2, Y: 4, Z: -2}, Size: 1.0, FloatSpeed: 0.45, RotSpeed: 0.2},
				{Offset: WorldVec3{X: 2, Y: 4, Z: -2}, Size: 1.0, FloatSpeed: 0.45, RotSpeed: 0.2},
				{Offset: WorldVec3{X: 2, Y: 4, Z: 2}, Size: 1.0, FloatSpeed: 0.45, RotSpeed: 0.2},
				{Offset: WorldVec3{X: -2, Y: 4, Z: 2}, Size: 1.0, FloatSpeed: 0.45, RotSpeed: 0.2},
				{Offset: WorldVec3{X: 0, Y: 6.5, Z: 0}, Size: 0.7, FloatSpeed: 0.6, RotSpeed: 0.35},
			},
			EntitySlots: generateSlots(WorldVec3{X: -16, Y: 0, Z: -22}, 7, 8),
		},
		{
			Kind:     company.ZoneKindResultsArchive,
			Name:     "Postmaster Keep",
			Subtitle: "Mail, outputs, and finished work",
			Accent:   "#0f8f8f",
			Position: WorldVec3{X: -8, Y: 0, Z: 28},
			LabelY:   9,
			Cubes: []CubeDef{
				{Offset: WorldVec3{X: -2, Y: 2, Z: 0}, Size: 1.4, FloatSpeed: 0.4, RotSpeed: 0.15},
				{Offset: WorldVec3{X: -0.5, Y: 3.5, Z: 0.5}, Size: 1.2, FloatSpeed: 0.45, RotSpeed: 0.18},
				{Offset: WorldVec3{X: 1, Y: 5, Z: -0.3}, Size: 1.0, FloatSpeed: 0.5, RotSpeed: 0.22},
				{Offset: WorldVec3{X: 2.2, Y: 6.5, Z: 0.8}, Size: 0.8, FloatSpeed: 0.55, RotSpeed: 0.25},
				{Offset: WorldVec3{X: 3, Y: 8, Z: 0}, Size: 0.6, FloatSpeed: 0.6, RotSpeed: 0.3},
			},
			EntitySlots: generateSlots(WorldVec3{X: -8, Y: 0, Z: 28}, 7, 8),
		},
	}
}

func generateSlots(center WorldVec3, radius float64, count int) []WorldVec3 {
	slots := make([]WorldVec3, 0, count)
	for i := 0; i < count; i++ {
		angle := float64(i)*1.1 + 0.3*math.Pi
		r := radius + float64(i%4)*2
		slots = append(slots, WorldVec3{
			X: center.X + math.Cos(angle)*r,
			Y: 0,
			Z: center.Z + math.Sin(angle)*r,
		})
	}
	return slots
}

func defaultDecorations() []DecorationDef {
	return []DecorationDef{
		// Inner formations
		{Position: WorldVec3{X: 6, Y: 0, Z: 6}, BaseSize: 1.2, Steps: 3, Shrink: 0.7, Jitter: 0.15},
		{Position: WorldVec3{X: -5, Y: 0, Z: -6}, BaseSize: 1.0, Steps: 2, Shrink: 0.65, Jitter: 0.1},
		{Position: WorldVec3{X: 10, Y: 0, Z: -8}, BaseSize: 0.9, Steps: 3, Shrink: 0.6, Jitter: 0.1},
		{Position: WorldVec3{X: -8, Y: 0, Z: 12}, BaseSize: 1.1, Steps: 2, Shrink: 0.7, Jitter: 0.12},
		{Position: WorldVec3{X: 4, Y: 0, Z: -14}, BaseSize: 1.3, Steps: 3, Shrink: 0.65, Jitter: 0.15},
		{Position: WorldVec3{X: -12, Y: 0, Z: -4}, BaseSize: 0.8, Steps: 2, Shrink: 0.6, Jitter: 0.08},
		{Position: WorldVec3{X: 18, Y: 0, Z: 10}, BaseSize: 1.0, Steps: 3, Shrink: 0.65, Jitter: 0.12},
		{Position: WorldVec3{X: -6, Y: 0, Z: 20}, BaseSize: 1.2, Steps: 2, Shrink: 0.7, Jitter: 0.1},
		{Position: WorldVec3{X: 22, Y: 0, Z: -10}, BaseSize: 0.9, Steps: 2, Shrink: 0.6, Jitter: 0.08},
		{Position: WorldVec3{X: -18, Y: 0, Z: -14}, BaseSize: 1.1, Steps: 3, Shrink: 0.65, Jitter: 0.1},

		// Mid-ring
		{Position: WorldVec3{X: 38, Y: 0, Z: 5}, BaseSize: 2.5, Steps: 6, Shrink: 0.72, Jitter: 0.3},
		{Position: WorldVec3{X: -36, Y: 0, Z: -10}, BaseSize: 2.8, Steps: 7, Shrink: 0.7, Jitter: 0.35},
		{Position: WorldVec3{X: 10, Y: 0, Z: 38}, BaseSize: 2.2, Steps: 5, Shrink: 0.68, Jitter: 0.25},
		{Position: WorldVec3{X: -30, Y: 0, Z: 20}, BaseSize: 2.0, Steps: 5, Shrink: 0.7, Jitter: 0.2},
		{Position: WorldVec3{X: 35, Y: 0, Z: -18}, BaseSize: 2.6, Steps: 6, Shrink: 0.72, Jitter: 0.3},
		{Position: WorldVec3{X: -15, Y: 0, Z: -35}, BaseSize: 2.4, Steps: 6, Shrink: 0.7, Jitter: 0.28},
		{Position: WorldVec3{X: 30, Y: 0, Z: 25}, BaseSize: 1.8, Steps: 5, Shrink: 0.68, Jitter: 0.2},
		{Position: WorldVec3{X: -32, Y: 0, Z: 10}, BaseSize: 2.0, Steps: 4, Shrink: 0.72, Jitter: 0.22},
		{Position: WorldVec3{X: 20, Y: 0, Z: -32}, BaseSize: 2.2, Steps: 5, Shrink: 0.7, Jitter: 0.25},
		{Position: WorldVec3{X: -10, Y: 0, Z: 36}, BaseSize: 1.9, Steps: 4, Shrink: 0.68, Jitter: 0.2},
		{Position: WorldVec3{X: 40, Y: 0, Z: -5}, BaseSize: 1.6, Steps: 4, Shrink: 0.65, Jitter: 0.18},
		{Position: WorldVec3{X: -38, Y: 0, Z: -25}, BaseSize: 2.0, Steps: 5, Shrink: 0.7, Jitter: 0.22},

		// Outer ring
		{Position: WorldVec3{X: 55, Y: 0, Z: 15}, BaseSize: 4.0, Steps: 8, Shrink: 0.74, Jitter: 0.5},
		{Position: WorldVec3{X: -50, Y: 0, Z: -20}, BaseSize: 4.5, Steps: 9, Shrink: 0.72, Jitter: 0.6},
		{Position: WorldVec3{X: 20, Y: 0, Z: 55}, BaseSize: 3.5, Steps: 7, Shrink: 0.7, Jitter: 0.4},
		{Position: WorldVec3{X: -45, Y: 0, Z: 35}, BaseSize: 3.8, Steps: 8, Shrink: 0.73, Jitter: 0.5},
		{Position: WorldVec3{X: 50, Y: 0, Z: -30}, BaseSize: 4.2, Steps: 8, Shrink: 0.72, Jitter: 0.55},
		{Position: WorldVec3{X: -25, Y: 0, Z: -50}, BaseSize: 3.6, Steps: 7, Shrink: 0.7, Jitter: 0.45},
		{Position: WorldVec3{X: 60, Y: 0, Z: -15}, BaseSize: 3.0, Steps: 6, Shrink: 0.68, Jitter: 0.35},
		{Position: WorldVec3{X: -55, Y: 0, Z: 10}, BaseSize: 3.2, Steps: 7, Shrink: 0.7, Jitter: 0.4},

		// Far background
		{Position: WorldVec3{X: 75, Y: 0, Z: 30}, BaseSize: 6.0, Steps: 10, Shrink: 0.76, Jitter: 0.8},
		{Position: WorldVec3{X: -70, Y: 0, Z: -40}, BaseSize: 6.5, Steps: 11, Shrink: 0.75, Jitter: 0.9},
		{Position: WorldVec3{X: 40, Y: 0, Z: 70}, BaseSize: 5.5, Steps: 9, Shrink: 0.74, Jitter: 0.7},
		{Position: WorldVec3{X: -60, Y: 0, Z: 55}, BaseSize: 5.0, Steps: 9, Shrink: 0.73, Jitter: 0.65},
		{Position: WorldVec3{X: 70, Y: 0, Z: -50}, BaseSize: 5.8, Steps: 10, Shrink: 0.75, Jitter: 0.75},
		{Position: WorldVec3{X: -40, Y: 0, Z: -65}, BaseSize: 5.2, Steps: 9, Shrink: 0.74, Jitter: 0.7},
		{Position: WorldVec3{X: 80, Y: 0, Z: 0}, BaseSize: 4.5, Steps: 8, Shrink: 0.72, Jitter: 0.55},
		{Position: WorldVec3{X: 0, Y: 0, Z: -75}, BaseSize: 5.0, Steps: 9, Shrink: 0.73, Jitter: 0.65},
		{Position: WorldVec3{X: 0, Y: 0, Z: 75}, BaseSize: 4.8, Steps: 8, Shrink: 0.73, Jitter: 0.6},
		{Position: WorldVec3{X: -75, Y: 0, Z: 0}, BaseSize: 5.5, Steps: 10, Shrink: 0.74, Jitter: 0.7},
	}
}

