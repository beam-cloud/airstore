package tools

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// ToolSchema defines the structure of a tool definition YAML file
type ToolSchema struct {
	Name        string                    `yaml:"name"`
	Description string                    `yaml:"description"`
	Icon        string                    `yaml:"icon,omitempty"`
	Commands    map[string]*CommandSchema `yaml:"commands"`
}

// CommandSchema defines a single command within a tool
type CommandSchema struct {
	Description string         `yaml:"description"`
	Params      []*ParamSchema `yaml:"params,omitempty"`
}

// ParamSchema defines a parameter for a command
type ParamSchema struct {
	Name        string `yaml:"name"`
	Type        string `yaml:"type"`                  // string, int, bool, float
	Required    bool   `yaml:"required,omitempty"`    // default false
	Position    *int   `yaml:"position,omitempty"`    // positional arg index (nil = flag only)
	Short       string `yaml:"short,omitempty"`       // short flag (e.g., "-n")
	Flag        string `yaml:"flag,omitempty"`        // long flag (e.g., "--limit")
	Default     any    `yaml:"default,omitempty"`     // default value
	Description string `yaml:"description,omitempty"` // help text
}

// LoadSchema loads a tool schema from a YAML file
func LoadSchema(path string) (*ToolSchema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read schema file: %w", err)
	}

	return ParseSchema(data)
}

// ParseSchema parses a tool schema from YAML bytes
func ParseSchema(data []byte) (*ToolSchema, error) {
	var schema ToolSchema
	if err := yaml.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("parse schema yaml: %w", err)
	}

	if err := schema.Validate(); err != nil {
		return nil, fmt.Errorf("invalid schema: %w", err)
	}

	return &schema, nil
}

// Validate checks that the schema is well-formed
func (s *ToolSchema) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("name is required")
	}
	if s.Description == "" {
		return fmt.Errorf("description is required")
	}
	if len(s.Commands) == 0 {
		return fmt.Errorf("at least one command is required")
	}

	for cmdName, cmd := range s.Commands {
		if cmd.Description == "" {
			return fmt.Errorf("command %q: description is required", cmdName)
		}
		if err := cmd.ValidateParams(); err != nil {
			return fmt.Errorf("command %q: %w", cmdName, err)
		}
	}

	return nil
}

// ValidateParams checks that command parameters are well-formed
func (c *CommandSchema) ValidateParams() error {
	positions := make(map[int]string)
	flags := make(map[string]string)

	for _, p := range c.Params {
		if p.Name == "" {
			return fmt.Errorf("param name is required")
		}

		// Validate type
		switch p.Type {
		case "string", "int", "bool", "float":
			// valid
		case "":
			return fmt.Errorf("param %q: type is required", p.Name)
		default:
			return fmt.Errorf("param %q: unknown type %q", p.Name, p.Type)
		}

		// Check for duplicate positions
		if p.Position != nil {
			if existing, ok := positions[*p.Position]; ok {
				return fmt.Errorf("param %q: position %d already used by %q", p.Name, *p.Position, existing)
			}
			positions[*p.Position] = p.Name
		}

		// Check for duplicate flags
		if p.Flag != "" {
			if existing, ok := flags[p.Flag]; ok {
				return fmt.Errorf("param %q: flag %q already used by %q", p.Name, p.Flag, existing)
			}
			flags[p.Flag] = p.Name
		}

		// Required params should have position or flag
		if p.Required && p.Position == nil && p.Flag == "" {
			return fmt.Errorf("param %q: required param must have position or flag", p.Name)
		}
	}

	return nil
}

// GetPositionalParams returns params sorted by position
func (c *CommandSchema) GetPositionalParams() []*ParamSchema {
	var positional []*ParamSchema
	for _, p := range c.Params {
		if p.Position != nil {
			positional = append(positional, p)
		}
	}

	// Sort by position
	for i := 0; i < len(positional)-1; i++ {
		for j := i + 1; j < len(positional); j++ {
			if *positional[i].Position > *positional[j].Position {
				positional[i], positional[j] = positional[j], positional[i]
			}
		}
	}

	return positional
}

// GetFlagParams returns params that have flags
func (c *CommandSchema) GetFlagParams() []*ParamSchema {
	var flags []*ParamSchema
	for _, p := range c.Params {
		if p.Flag != "" {
			flags = append(flags, p)
		}
	}
	return flags
}

// DiscoverSchemas finds all .yaml files in a directory and loads them
func DiscoverSchemas(dir string) ([]*ToolSchema, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read tools directory: %w", err)
	}

	var schemas []*ToolSchema
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		ext := filepath.Ext(entry.Name())
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		schema, err := LoadSchema(path)
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", entry.Name(), err)
		}

		schemas = append(schemas, schema)
	}

	return schemas, nil
}
