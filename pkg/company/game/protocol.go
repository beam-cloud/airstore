package game

import (
	"encoding/json"

	"github.com/beam-cloud/airstore/pkg/company"
)

type Opcode string

const (
	// Server → Client
	OpcodeWorldMap      Opcode = "world_map"
	OpcodeWorldSnapshot Opcode = "world_snapshot"
	OpcodeWorldDelta    Opcode = "world_delta"
	OpcodeHeartbeat     Opcode = "heartbeat"
	OpcodePong          Opcode = "pong"

	// Client → Server
	OpcodePing Opcode = "ping"
)

type GameMessage struct {
	Op        Opcode          `json:"op"`
	Seq       int64           `json:"seq,omitempty"`
	Timestamp int64           `json:"ts"`
	Data      json.RawMessage `json:"d,omitempty"`
}

// Server → Client payloads

type WorldMapPayload struct {
	Map *WorldMap `json:"map"`
}

type WorldSnapshotPayload struct {
	Snapshot *company.CompanyWorldSnapshot `json:"snapshot"`
}

type WorldDeltaPayload struct {
	Delta *company.CompanyWorldDelta `json:"delta"`
}

// Helpers

func NewGameMessage(op Opcode, data any) (GameMessage, error) {
	var raw json.RawMessage
	if data != nil {
		b, err := json.Marshal(data)
		if err != nil {
			return GameMessage{}, err
		}
		raw = b
	}
	return GameMessage{
		Op:        op,
		Timestamp: nowMs(),
		Data:      raw,
	}, nil
}

func MustGameMessage(op Opcode, data any) GameMessage {
	msg, err := NewGameMessage(op, data)
	if err != nil {
		panic(err)
	}
	return msg
}
