package game

import (
	"math"

	"github.com/beam-cloud/airstore/pkg/company"
)

// ---------------------------------------------------------------------------
// World Map — backend-authoritative world definition
// ---------------------------------------------------------------------------

type WorldVec3 struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

type WorldMap struct {
	Terrain     TerrainMap       `json:"terrain"`
	Zones       []ZoneDefinition `json:"zones"`
	Decorations []DecorationDef  `json:"decorations"`
	EntityTypes []EntityTypeDef  `json:"entity_types"`
	Spawn       WorldVec3        `json:"spawn"`
}

// ---------------------------------------------------------------------------
// Terrain — heightmap-based region system
// ---------------------------------------------------------------------------

type TerrainRegion struct {
	GridX      int       `json:"grid_x"`
	GridZ      int       `json:"grid_z"`
	Size       float64   `json:"size"`
	Resolution int       `json:"resolution"`
	Heights    []float64 `json:"heights"` // row-major, len = Resolution * Resolution
}

type LandmarkPeak struct {
	X      float64 `json:"x"`
	Z      float64 `json:"z"`
	Height float64 `json:"height"`
	Radius float64 `json:"radius"`
}

type TerrainMap struct {
	RegionSize float64         `json:"region_size"`
	Regions    []TerrainRegion `json:"regions"`
	Peaks      []LandmarkPeak  `json:"peaks"`
	BaseColor  string          `json:"base_color"`
	GridColor  string          `json:"grid_color"`
	FogNear    float64         `json:"fog_near"`
	FogFar     float64         `json:"fog_far"`
	FogColor   string          `json:"fog_color"`
}

// ---------------------------------------------------------------------------
// Zone / decoration / entity types
// ---------------------------------------------------------------------------

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
	Shape  string             `json:"shape"`
	Size   float64            `json:"size"`
	FloatY float64            `json:"float_y"`
}

// ---------------------------------------------------------------------------
// Noise — sine-hash fbm for terrain generation
// ---------------------------------------------------------------------------

func noiseHash(x, z float64) float64 {
	v := math.Sin(x*127.1+z*311.7) * 43758.5453
	return v - math.Floor(v)
}

func smoothNoise(x, z float64) float64 {
	ix := math.Floor(x)
	iz := math.Floor(z)
	fx := x - ix
	fz := z - iz
	fx = fx * fx * (3 - 2*fx)
	fz = fz * fz * (3 - 2*fz)

	a := noiseHash(ix, iz)
	b := noiseHash(ix+1, iz)
	c := noiseHash(ix, iz+1)
	d := noiseHash(ix+1, iz+1)

	return a*(1-fx)*(1-fz) + b*fx*(1-fz) + c*(1-fx)*fz + d*fx*fz
}

func terrainFBM(x, z float64) float64 {
	var total float64
	amplitude := 1.0
	frequency := 1.0
	for i := 0; i < 4; i++ {
		total += smoothNoise(x*frequency*0.02, z*frequency*0.02) * amplitude
		amplitude *= 0.5
		frequency *= 2.0
	}
	return total * 1.8
}

func sumPeaks(x, z float64, peaks []LandmarkPeak) float64 {
	var h float64
	for _, p := range peaks {
		dx := x - p.X
		dz := z - p.Z
		d2 := dx*dx + dz*dz
		r2 := 2 * p.Radius * p.Radius
		h += p.Height * math.Exp(-d2/r2)
	}
	return h
}

// TerrainHeight computes the world height at any (x,z) coordinate.
// Useful for placing objects and computing spawn Y.
func TerrainHeight(x, z float64, peaks []LandmarkPeak) float64 {
	return terrainFBM(x, z) + sumPeaks(x, z, peaks)
}

// ---------------------------------------------------------------------------
// Terrain generation
// ---------------------------------------------------------------------------

func generateTerrainMap() TerrainMap {
	const regionSize = 100.0
	const resolution = 33

	peaks := defaultPeaks()
	regions := make([]TerrainRegion, 0, 9)

	for gz := -1; gz <= 1; gz++ {
		for gx := -1; gx <= 1; gx++ {
			originX := float64(gx) * regionSize
			originZ := float64(gz) * regionSize
			heights := make([]float64, resolution*resolution)

			for zi := 0; zi < resolution; zi++ {
				for xi := 0; xi < resolution; xi++ {
					t := float64(xi) / float64(resolution-1)
					u := float64(zi) / float64(resolution-1)
					wx := originX + t*regionSize
					wz := originZ + u*regionSize
					heights[zi*resolution+xi] = TerrainHeight(wx, wz, peaks)
				}
			}

			regions = append(regions, TerrainRegion{
				GridX:      gx,
				GridZ:      gz,
				Size:       regionSize,
				Resolution: resolution,
				Heights:    heights,
			})
		}
	}

	return TerrainMap{
		RegionSize: regionSize,
		Regions:    regions,
		Peaks:      peaks,
		BaseColor:  "#e4e8f0",
		GridColor:  "#b8c4d8",
		FogNear:    40,
		FogFar:     160,
		FogColor:   "#e8ecf4",
	}
}

func defaultPeaks() []LandmarkPeak {
	return []LandmarkPeak{
		{X: 75, Z: 30, Height: 22, Radius: 12},
		{X: -70, Z: -40, Height: 25, Radius: 14},
		{X: 40, Z: 70, Height: 18, Radius: 10},
		{X: -60, Z: 55, Height: 20, Radius: 11},
		{X: 70, Z: -50, Height: 23, Radius: 13},
		{X: -40, Z: -65, Height: 19, Radius: 10},
		{X: 80, Z: 0, Height: 16, Radius: 9},
		{X: 0, Z: -75, Height: 20, Radius: 12},
		{X: 0, Z: 75, Height: 17, Radius: 10},
		{X: -75, Z: 0, Height: 21, Radius: 12},
	}
}

// ---------------------------------------------------------------------------
// Default world layout
// ---------------------------------------------------------------------------

func DefaultWorldMap() *WorldMap {
	terrain := generateTerrainMap()
	spawnX, spawnZ := 0.0, 35.0
	spawnY := TerrainHeight(spawnX, spawnZ, terrain.Peaks) + 0.6

	return &WorldMap{
		Terrain: terrain,
		Spawn:   WorldVec3{X: spawnX, Y: spawnY, Z: spawnZ},
		Zones:   defaultZones(),
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
			// Town hall tower: wide foundation platform with progressively smaller cubes stacked upward
			Kind:     company.ZoneKindCommandCenter,
			Name:     "Town Square",
			Subtitle: "Direct agents and review the city's pulse",
			Accent:   "#1565c0",
			Position: WorldVec3{X: 0, Y: 0, Z: 0},
			LabelY:   12,
			Cubes: []CubeDef{
				{Offset: WorldVec3{X: 0, Y: 1, Z: 0}, Size: 3.0, FloatSpeed: 0.25, RotSpeed: 0.06},
				{Offset: WorldVec3{X: 0, Y: 3.5, Z: 0}, Size: 2.2, FloatSpeed: 0.3, RotSpeed: 0.1},
				{Offset: WorldVec3{X: 0, Y: 6, Z: 0}, Size: 1.6, FloatSpeed: 0.38, RotSpeed: 0.14},
				{Offset: WorldVec3{X: 0, Y: 8, Z: 0}, Size: 1.0, FloatSpeed: 0.48, RotSpeed: 0.2},
				{Offset: WorldVec3{X: 0, Y: 9.5, Z: 0}, Size: 0.6, FloatSpeed: 0.6, RotSpeed: 0.3},
			},
			EntitySlots: generateSlots(WorldVec3{X: 0, Y: 0, Z: 0}, 7, 12),
		},
		{
			// Three-bay workshop: three base cubes in a row with stacked upper sections and roof accents
			Kind:     company.ZoneKindActiveOps,
			Name:     "Operations Ward",
			Subtitle: "Live work, casts, and active quests",
			Accent:   "#00b894",
			Position: WorldVec3{X: 28, Y: 0, Z: -5},
			LabelY:   8,
			Cubes: []CubeDef{
				{Offset: WorldVec3{X: -3, Y: 2, Z: 0}, Size: 1.8, FloatSpeed: 0.35, RotSpeed: 0.1},
				{Offset: WorldVec3{X: 0, Y: 2.5, Z: 0}, Size: 2.0, FloatSpeed: 0.3, RotSpeed: 0.08},
				{Offset: WorldVec3{X: 3, Y: 2, Z: 0}, Size: 1.8, FloatSpeed: 0.35, RotSpeed: 0.1},
				{Offset: WorldVec3{X: -3, Y: 4.5, Z: 0}, Size: 1.0, FloatSpeed: 0.5, RotSpeed: 0.2},
				{Offset: WorldVec3{X: 0, Y: 5, Z: 0}, Size: 1.2, FloatSpeed: 0.45, RotSpeed: 0.16},
				{Offset: WorldVec3{X: 3, Y: 4.5, Z: 0}, Size: 1.0, FloatSpeed: 0.5, RotSpeed: 0.2},
				{Offset: WorldVec3{X: -1.5, Y: 6.5, Z: 0}, Size: 0.6, FloatSpeed: 0.6, RotSpeed: 0.3},
				{Offset: WorldVec3{X: 1.5, Y: 6.5, Z: 0}, Size: 0.6, FloatSpeed: 0.6, RotSpeed: 0.3},
			},
			EntitySlots: generateSlots(WorldVec3{X: 28, Y: 0, Z: -5}, 7, 12),
		},
		{
			// Tall spire: single vertical stack, largest at bottom, smallest at top
			Kind:     company.ZoneKindAttentionTower,
			Name:     "Watchtower",
			Subtitle: "Inbox alerts, blockers, and errors",
			Accent:   "#c62828",
			Position: WorldVec3{X: -24, Y: 0, Z: 8},
			LabelY:   13,
			Cubes: []CubeDef{
				{Offset: WorldVec3{X: 0, Y: 2, Z: 0}, Size: 2.0, FloatSpeed: 0.28, RotSpeed: 0.08},
				{Offset: WorldVec3{X: 0, Y: 5, Z: 0}, Size: 1.5, FloatSpeed: 0.38, RotSpeed: 0.14},
				{Offset: WorldVec3{X: 0, Y: 7.5, Z: 0}, Size: 1.1, FloatSpeed: 0.48, RotSpeed: 0.2},
				{Offset: WorldVec3{X: 0, Y: 9.5, Z: 0}, Size: 0.8, FloatSpeed: 0.55, RotSpeed: 0.25},
				{Offset: WorldVec3{X: 0, Y: 11, Z: 0}, Size: 0.5, FloatSpeed: 0.65, RotSpeed: 0.35},
			},
			EntitySlots: generateSlots(WorldVec3{X: -24, Y: 0, Z: 8}, 7, 8),
		},
		{
			// Market stalls: central pavilion cube with low stalls scattered around it
			Kind:     company.ZoneKindSourceDistrict,
			Name:     "Source Bazaar",
			Subtitle: "Connected systems, tools, and vendors",
			Accent:   "#dc8b4f",
			Position: WorldVec3{X: 16, Y: 0, Z: 24},
			LabelY:   7,
			Cubes: []CubeDef{
				{Offset: WorldVec3{X: 0, Y: 3, Z: 0}, Size: 1.5, FloatSpeed: 0.35, RotSpeed: 0.12},
				{Offset: WorldVec3{X: -2.5, Y: 1.5, Z: -2}, Size: 0.8, FloatSpeed: 0.5, RotSpeed: 0.22},
				{Offset: WorldVec3{X: 2, Y: 2, Z: -1.5}, Size: 0.7, FloatSpeed: 0.55, RotSpeed: 0.25},
				{Offset: WorldVec3{X: -1.5, Y: 1.8, Z: 2.5}, Size: 0.9, FloatSpeed: 0.48, RotSpeed: 0.2},
				{Offset: WorldVec3{X: 2.5, Y: 1.5, Z: 2}, Size: 0.75, FloatSpeed: 0.52, RotSpeed: 0.24},
				{Offset: WorldVec3{X: -3, Y: 2, Z: 0.5}, Size: 0.65, FloatSpeed: 0.58, RotSpeed: 0.28},
				{Offset: WorldVec3{X: 3, Y: 1.8, Z: -0.5}, Size: 0.7, FloatSpeed: 0.52, RotSpeed: 0.24},
				{Offset: WorldVec3{X: 0, Y: 5, Z: 0}, Size: 0.8, FloatSpeed: 0.55, RotSpeed: 0.28},
			},
			EntitySlots: generateSlots(WorldVec3{X: 16, Y: 0, Z: 24}, 7, 12),
		},
		{
			// Structured hall: four corner pillars with bridge cubes and central spire
			Kind:     company.ZoneKindSchedulingHall,
			Name:     "Clockwork Hall",
			Subtitle: "Timers, wakes, and queued follow-ups",
			Accent:   "#7c5cbf",
			Position: WorldVec3{X: -16, Y: 0, Z: -22},
			LabelY:   9,
			Cubes: []CubeDef{
				{Offset: WorldVec3{X: -2.5, Y: 3, Z: -2.5}, Size: 1.2, FloatSpeed: 0.35, RotSpeed: 0.12},
				{Offset: WorldVec3{X: 2.5, Y: 3, Z: -2.5}, Size: 1.2, FloatSpeed: 0.35, RotSpeed: 0.12},
				{Offset: WorldVec3{X: 2.5, Y: 3, Z: 2.5}, Size: 1.2, FloatSpeed: 0.35, RotSpeed: 0.12},
				{Offset: WorldVec3{X: -2.5, Y: 3, Z: 2.5}, Size: 1.2, FloatSpeed: 0.35, RotSpeed: 0.12},
				{Offset: WorldVec3{X: 0, Y: 4, Z: -2.5}, Size: 0.7, FloatSpeed: 0.5, RotSpeed: 0.22},
				{Offset: WorldVec3{X: 2.5, Y: 4, Z: 0}, Size: 0.7, FloatSpeed: 0.5, RotSpeed: 0.22},
				{Offset: WorldVec3{X: 0, Y: 4, Z: 2.5}, Size: 0.7, FloatSpeed: 0.5, RotSpeed: 0.22},
				{Offset: WorldVec3{X: -2.5, Y: 4, Z: 0}, Size: 0.7, FloatSpeed: 0.5, RotSpeed: 0.22},
				{Offset: WorldVec3{X: 0, Y: 6.5, Z: 0}, Size: 0.9, FloatSpeed: 0.45, RotSpeed: 0.28},
			},
			EntitySlots: generateSlots(WorldVec3{X: -16, Y: 0, Z: -22}, 7, 8),
		},
		{
			// Terraced archive: ascending staircase of cubes from left to right
			Kind:     company.ZoneKindResultsArchive,
			Name:     "Postmaster Keep",
			Subtitle: "Mail, outputs, and finished work",
			Accent:   "#0f8f8f",
			Position: WorldVec3{X: -8, Y: 0, Z: 28},
			LabelY:   11,
			Cubes: []CubeDef{
				{Offset: WorldVec3{X: -3, Y: 1.5, Z: 0}, Size: 1.6, FloatSpeed: 0.3, RotSpeed: 0.1},
				{Offset: WorldVec3{X: -1.5, Y: 3, Z: 0.5}, Size: 1.4, FloatSpeed: 0.35, RotSpeed: 0.13},
				{Offset: WorldVec3{X: 0, Y: 4.5, Z: 0}, Size: 1.2, FloatSpeed: 0.4, RotSpeed: 0.16},
				{Offset: WorldVec3{X: 1.5, Y: 6, Z: -0.3}, Size: 1.0, FloatSpeed: 0.45, RotSpeed: 0.2},
				{Offset: WorldVec3{X: 3, Y: 7.5, Z: 0.2}, Size: 0.8, FloatSpeed: 0.5, RotSpeed: 0.24},
				{Offset: WorldVec3{X: 4, Y: 9, Z: 0}, Size: 0.6, FloatSpeed: 0.58, RotSpeed: 0.3},
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
		// Inner formations — small cubic features near zones
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

		// Mid-ring — medium features between zones and peaks
		{Position: WorldVec3{X: 38, Y: 0, Z: 5}, BaseSize: 2.5, Steps: 6, Shrink: 0.72, Jitter: 0.3},
		{Position: WorldVec3{X: -36, Y: 0, Z: -10}, BaseSize: 2.8, Steps: 7, Shrink: 0.7, Jitter: 0.35},
		{Position: WorldVec3{X: 10, Y: 0, Z: 38}, BaseSize: 2.2, Steps: 5, Shrink: 0.68, Jitter: 0.25},
		{Position: WorldVec3{X: -30, Y: 0, Z: 20}, BaseSize: 2.0, Steps: 5, Shrink: 0.7, Jitter: 0.2},
		{Position: WorldVec3{X: 35, Y: 0, Z: -18}, BaseSize: 2.6, Steps: 6, Shrink: 0.72, Jitter: 0.3},
		{Position: WorldVec3{X: -15, Y: 0, Z: -35}, BaseSize: 2.4, Steps: 6, Shrink: 0.7, Jitter: 0.28},
		{Position: WorldVec3{X: 30, Y: 0, Z: 25}, BaseSize: 1.8, Steps: 5, Shrink: 0.68, Jitter: 0.2},
		{Position: WorldVec3{X: -32, Y: 0, Z: 10}, BaseSize: 2.0, Steps: 4, Shrink: 0.72, Jitter: 0.22},
	}
}
