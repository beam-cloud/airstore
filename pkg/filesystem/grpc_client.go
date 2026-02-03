package filesystem

import (
	"context"
	"errors"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

// GRPCConfig holds configuration for connecting to the gateway.
type GRPCConfig struct {
	GatewayAddr string
	Token       string
}

// GRPCMetadataEngine implements MetadataEngine and LegacyMetadataEngine via gRPC.
type GRPCMetadataEngine struct {
	conn   *grpc.ClientConn
	client pb.FilesystemServiceClient
	token  string
}

// NewGRPCMetadataEngine creates a new gRPC-based metadata engine.
func NewGRPCMetadataEngine(cfg GRPCConfig) (*GRPCMetadataEngine, error) {
	// Keepalive parameters to maintain connection health and detect failures quickly.
	// This reduces latency by keeping connections alive and avoiding reconnection overhead.
	// Note: Time must be >= server's MinTime (often 5 minutes) to avoid ENHANCE_YOUR_CALM.
	keepaliveParams := keepalive.ClientParameters{
		Time:                60 * time.Second, // Send pings every 60s if no activity
		Timeout:             10 * time.Second, // Wait 10s for ping ack before assuming dead
		PermitWithoutStream: true,             // Send pings even without active streams
	}

	conn, err := grpc.NewClient(
		cfg.GatewayAddr,
		grpc.WithTransportCredentials(common.TransportCredentials(cfg.GatewayAddr)),
		grpc.WithKeepaliveParams(keepaliveParams),
	)
	if err != nil {
		return nil, err
	}

	return &GRPCMetadataEngine{
		conn:   conn,
		client: pb.NewFilesystemServiceClient(conn),
		token:  cfg.Token,
	}, nil
}

func (m *GRPCMetadataEngine) ctx() context.Context {
	ctx := context.Background()
	if m.token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+m.token)
	}
	return ctx
}

// Close closes the gRPC connection.
func (m *GRPCMetadataEngine) Close() error {
	return m.conn.Close()
}

// Conn returns the underlying gRPC connection for sharing with other services.
func (m *GRPCMetadataEngine) Conn() *grpc.ClientConn {
	return m.conn
}

// ===== LegacyMetadataEngine implementation =====
// These methods provide backward compatibility with the old FUSE implementation.

func (m *GRPCMetadataEngine) GetDirectoryAccessMetadata(pid, name string) (*DirectoryAccessMetadata, error) {
	path := buildPath(pid, name)

	resp, err := m.client.Stat(m.ctx(), &pb.StatRequest{Path: path})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, errors.New(resp.Error)
	}

	return &DirectoryAccessMetadata{
		PID:        pid,
		ID:         path,
		Permission: resp.Info.Mode,
	}, nil
}

func (m *GRPCMetadataEngine) SaveDirectoryAccessMetadata(meta *DirectoryAccessMetadata) error {
	path := buildPath(meta.PID, meta.ID)
	
	resp, err := m.client.Mkdir(m.ctx(), &pb.MkdirRequest{
		Path: path,
		Mode: meta.Permission,
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return errors.New(resp.Error)
	}
	return nil
}

func (m *GRPCMetadataEngine) GetDirectoryContentMetadata(id string) (*DirectoryContentMetadata, error) {
	resp, err := m.client.ReadDir(m.ctx(), &pb.ReadDirRequest{Path: id})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, errors.New(resp.Error)
	}

	names := make([]string, len(resp.Entries))
	timestamps := make(map[string]time.Time)
	for i, e := range resp.Entries {
		names[i] = e.Name
		if e.Mtime > 0 {
			timestamps[e.Name] = time.Unix(e.Mtime, 0)
		}
	}

	return &DirectoryContentMetadata{
		Id:         id,
		EntryList:  names,
		Timestamps: timestamps,
	}, nil
}

func (m *GRPCMetadataEngine) SaveDirectoryContentMetadata(meta *DirectoryContentMetadata) error {
	// No-op: directory contents are derived from actual entries
	return nil
}

func (m *GRPCMetadataEngine) GetFileMetadata(pid, name string) (*FileMetadata, error) {
	path := buildPath(pid, name)

	resp, err := m.client.Stat(m.ctx(), &pb.StatRequest{Path: path})
	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, errors.New(resp.Error)
	}

	return &FileMetadata{
		ID:   path,
		PID:  pid,
		Name: name,
	}, nil
}

func (m *GRPCMetadataEngine) SaveFileMetadata(meta *FileMetadata) error {
	path := buildPath(meta.PID, meta.Name)
	
	resp, err := m.client.Create(m.ctx(), &pb.CreateRequest{
		Path: path,
		Mode: syscall.S_IFREG | 0644,
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return errors.New(resp.Error)
	}
	return nil
}

func (m *GRPCMetadataEngine) ListDirectory(path string) []DirEntry {
	resp, err := m.client.ReadDir(m.ctx(), &pb.ReadDirRequest{Path: path})
	if err != nil || !resp.Ok {
		return nil
	}

	entries := make([]DirEntry, len(resp.Entries))
	for i, e := range resp.Entries {
		entries[i] = DirEntry{
			Name: e.Name,
			Mode: e.Mode,
		}
	}
	return entries
}

func (m *GRPCMetadataEngine) RenameDirectory(oldPID, oldName, newPID, newName string, version int) error {
	oldPath := buildPath(oldPID, oldName)
	newPath := buildPath(newPID, newName)

	resp, err := m.client.Rename(m.ctx(), &pb.RenameRequest{
		OldPath: oldPath,
		NewPath: newPath,
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return errors.New(resp.Error)
	}
	return nil
}

func (m *GRPCMetadataEngine) DeleteDirectory(parentID, name string, version int) error {
	path := buildPath(parentID, name)

	resp, err := m.client.Remove(m.ctx(), &pb.RemoveRequest{Path: path})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return errors.New(resp.Error)
	}
	return nil
}

// buildPath constructs a path from parent ID and name.
func buildPath(pid, name string) string {
	if pid == "" {
		return "/" + name
	}
	return pid + "/" + name
}
