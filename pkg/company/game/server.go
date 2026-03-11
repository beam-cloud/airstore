package game

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// ---------------------------------------------------------------------------
// GameServer — per-workspace WebSocket hub
// ---------------------------------------------------------------------------

type GameServer struct {
	mu       sync.Mutex
	hubs     map[string]*Hub
	worldRT  *WorldRuntime
	worldMap *WorldMap
}

func NewGameServer(worldRT *WorldRuntime) *GameServer {
	wm := DefaultWorldMap()
	return &GameServer{
		hubs:     make(map[string]*Hub),
		worldRT:  worldRT,
		worldMap: wm,
	}
}

func (gs *GameServer) hubFor(workspaceID string) *Hub {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	if h, ok := gs.hubs[workspaceID]; ok {
		return h
	}
	h := newHub(workspaceID, gs)
	gs.hubs[workspaceID] = h
	go h.run()
	return h
}

func (gs *GameServer) HandleUpgrade(w http.ResponseWriter, r *http.Request, workspaceID uint) error {
	wsConn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return fmt.Errorf("websocket upgrade: %w", err)
	}
	hub := gs.hubFor(fmt.Sprintf("%d", workspaceID))
	conn := newConnection(hub, wsConn)
	hub.register <- conn
	go conn.readPump()
	go conn.writePump()

	// Block until the connection closes so Echo doesn't finalize the hijacked connection.
	<-conn.done
	return nil
}

func (gs *GameServer) Broadcast(workspaceID string, msg GameMessage) {
	gs.mu.Lock()
	hub, ok := gs.hubs[workspaceID]
	gs.mu.Unlock()
	if ok {
		hub.broadcast <- msg
	}
}

// ---------------------------------------------------------------------------
// Hub — manages connections for a single workspace
// ---------------------------------------------------------------------------

type Hub struct {
	workspaceID string
	server      *GameServer
	connections map[*Connection]bool
	broadcast   chan GameMessage
	register    chan *Connection
	unregister  chan *Connection
	done        chan struct{}
}

func newHub(workspaceID string, server *GameServer) *Hub {
	return &Hub{
		workspaceID: workspaceID,
		server:      server,
		connections: make(map[*Connection]bool),
		broadcast:   make(chan GameMessage, 64),
		register:    make(chan *Connection),
		unregister:  make(chan *Connection),
		done:        make(chan struct{}),
	}
}

func (h *Hub) run() {
	for {
		select {
		case conn := <-h.register:
			h.connections[conn] = true
			h.onConnect(conn)
		case conn := <-h.unregister:
			if _, ok := h.connections[conn]; ok {
				delete(h.connections, conn)
				close(conn.send)
			}
		case msg := <-h.broadcast:
			for conn := range h.connections {
				select {
				case conn.send <- msg:
				default:
					delete(h.connections, conn)
					close(conn.send)
				}
			}
		case <-h.done:
			return
		}
	}
}

func (h *Hub) onConnect(conn *Connection) {
	mapMsg := MustGameMessage(OpcodeWorldMap, WorldMapPayload{Map: h.server.worldMap})
	select {
	case conn.send <- mapMsg:
	default:
	}

	ctx := context.Background()
	wid := h.workspaceID
	var workspaceIDUint uint
	fmt.Sscanf(wid, "%d", &workspaceIDUint)

	snapshot, _, err := h.server.worldRT.SyncWorkspace(ctx, workspaceIDUint)
	if err != nil {
		log.Error().Err(err).Str("workspace", wid).Msg("game server: failed to sync on connect")
		return
	}
	if snapshot != nil {
		snapMsg := MustGameMessage(OpcodeWorldSnapshot, WorldSnapshotPayload{Snapshot: snapshot})
		select {
		case conn.send <- snapMsg:
		default:
		}
	}
}

func (h *Hub) handleMessage(conn *Connection, msg GameMessage) {
	switch msg.Op {
	case OpcodePing:
		pong := MustGameMessage(OpcodePong, nil)
		select {
		case conn.send <- pong:
		default:
		}
	}
}

// ---------------------------------------------------------------------------
// Connection — per-client WebSocket wrapper
// ---------------------------------------------------------------------------

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	maxMsgSize = 32 * 1024
)

type Connection struct {
	hub    *Hub
	ws     *websocket.Conn
	send   chan GameMessage
	done   chan struct{}
	closed atomic.Bool
}

func newConnection(hub *Hub, ws *websocket.Conn) *Connection {
	return &Connection{
		hub:  hub,
		ws:   ws,
		send: make(chan GameMessage, 64),
		done: make(chan struct{}),
	}
}

func (c *Connection) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.ws.Close()
		close(c.done)
	}()
	c.ws.SetReadLimit(maxMsgSize)
	c.ws.SetReadDeadline(time.Now().Add(pongWait))
	c.ws.SetPongHandler(func(string) error {
		c.ws.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, raw, err := c.ws.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure, websocket.CloseNoStatusReceived) {
				log.Debug().Err(err).Msg("game ws: read error")
			}
			return
		}

		var msg GameMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			continue
		}
		c.hub.handleMessage(c, msg)
	}
}

func (c *Connection) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.ws.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.ws.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			data, err := json.Marshal(msg)
			if err != nil {
				return
			}
			if err := c.ws.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}

		case <-ticker.C:
			c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
