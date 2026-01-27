package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const rpcTimeout = 30 * time.Second

// Config mirrors the structure in vnode/config.go
type Config struct {
	GatewayAddr string `json:"gateway_addr"`
	Token       string `json:"token,omitempty"`
}

func main() {
	// Determine tool name from how we were invoked (like BusyBox)
	toolName := filepath.Base(os.Args[0])
	args := os.Args[1:]

	// Load config
	cfg := loadConfig()

	// Handle --help
	if len(args) > 0 && (args[0] == "--help" || args[0] == "-h") {
		if err := showHelp(cfg, toolName); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Execute the tool
	exitCode, err := executeTool(cfg, toolName, args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	os.Exit(exitCode)
}

func loadConfig() *Config {
	cfg := &Config{
		GatewayAddr: "localhost:1993",
	}

	// Try to read from /.airstore/config relative to the shim location
	shimPath := os.Args[0]

	if absPath, err := filepath.Abs(shimPath); err == nil {
		mountRoot := filepath.Dir(filepath.Dir(absPath))
		configPath := filepath.Join(mountRoot, ".airstore", "config")

		data, err := os.ReadFile(configPath)

		if err == nil {
			var fileCfg Config
			if json.Unmarshal(data, &fileCfg) == nil {
				cfg = &fileCfg
			}
		}
	}

	// Environment variables override
	if v := os.Getenv("AIRSTORE_GATEWAY"); v != "" {
		cfg.GatewayAddr = v
	}
	if v := os.Getenv("AIRSTORE_TOKEN"); v != "" {
		cfg.Token = v
	}

	return cfg
}

func connect(cfg *Config) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	if cfg.Token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(&tokenCredentials{token: cfg.Token}))
	}

	conn, err := grpc.NewClient(cfg.GatewayAddr, opts...)

	return conn, err
}

type tokenCredentials struct {
	token string
}

func (t *tokenCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + t.token,
	}, nil
}

func (t *tokenCredentials) RequireTransportSecurity() bool {
	return false
}

func showHelp(cfg *Config, toolName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	conn, err := connect(cfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	client := pb.NewToolServiceClient(conn)

	resp, err := client.GetToolHelp(ctx, &pb.GetToolHelpRequest{
		Name: toolName,
	})

	if err != nil {
		return fmt.Errorf("get help: %w", err)
	}

	if !resp.Ok {
		return fmt.Errorf("%s", resp.Error)
	}

	fmt.Println(resp.Help)
	os.Stdout.Sync()
	return nil
}

func executeTool(cfg *Config, toolName string, args []string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	conn, err := connect(cfg)
	if err != nil {
		return 1, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	client := pb.NewToolServiceClient(conn)

	stream, err := client.ExecuteTool(ctx, &pb.ExecuteToolRequest{
		Name: toolName,
		Args: args,
	})

	if err != nil {
		return 1, fmt.Errorf("execute: %w", err)
	}

	var exitCode int32
	for {
		resp, err := stream.Recv()

		if err == io.EOF {
			break
		}
		if err != nil {
			return 1, fmt.Errorf("stream: %w", err)
		}

		if len(resp.Data) > 0 {
			switch resp.Stream {
			case pb.ExecuteToolResponse_STDOUT:
				os.Stdout.Write(resp.Data)
				os.Stdout.Sync()
			case pb.ExecuteToolResponse_STDERR:
				os.Stderr.Write(resp.Data)
				os.Stderr.Sync()
			}
		}

		if resp.Done {
			exitCode = resp.ExitCode
			if resp.Error != "" {
				fmt.Fprintf(os.Stderr, "error: %s\n", resp.Error)
				os.Stderr.Sync()
			}
			break
		}
	}

	return int(exitCode), nil
}
