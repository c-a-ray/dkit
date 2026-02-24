package core

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// configDirOverride allows tests to redirect the global config directory.
// When empty, os.UserConfigDir() is used.
var configDirOverride string

// SetConfigDirForTest overrides the config directory for testing.
// Pass "" to restore default behavior.
func SetConfigDirForTest(dir string) {
	configDirOverride = dir
}

// FileConfig represents the contents of a dkit YAML configuration file
type FileConfig struct {
	Name            string           `yaml:"name,omitempty"`
	Files           []string         `yaml:"files,omitempty"`
	Delimiter       string           `yaml:"delimiter,omitempty"`
	Encoding        string           `yaml:"encoding,omitempty"`
	NoHeader        *bool            `yaml:"noHeader,omitempty"`
	Quiet           *bool            `yaml:"quiet,omitempty"`
	LazyQuotes      *bool            `yaml:"lazyQuotes,omitempty"`
	SkipStart       *int             `yaml:"skipStart,omitempty"`
	SkipEnd         *int             `yaml:"skipEnd,omitempty"`
	FieldsPerRecord string           `yaml:"fieldsPerRecord,omitempty"`
	FixedColumns    []FixedColumnDef `yaml:"fixedColumns,omitempty"`
}

// FixedColumnDef defines a named fixed-width column (0-based, exclusive end)
type FixedColumnDef struct {
	Name  string `yaml:"name"`
	Start int    `yaml:"start"`
	End   int    `yaml:"end"`
}

// GlobalState represents the contents of ~/.config/dkit/global.yaml
type GlobalState struct {
	Active  string            `yaml:"active,omitempty"`
	Configs map[string]string `yaml:"configs,omitempty"`
}

// GlobalConfigDir returns the path to the dkit config directory, creating it if needed.
func GlobalConfigDir() (string, error) {
	var base string
	if configDirOverride != "" {
		base = configDirOverride
	} else {
		var err error
		base, err = os.UserConfigDir()
		if err != nil {
			return "", fmt.Errorf("cannot determine config directory: %w", err)
		}
	}
	dir := filepath.Join(base, "dkit")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating config directory: %w", err)
	}
	return dir, nil
}

func globalStatePath() (string, error) {
	dir, err := GlobalConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "global.yaml"), nil
}

// LoadGlobalState reads and parses the global state file.
// Returns a zero-value GlobalState if the file does not exist.
func LoadGlobalState() (*GlobalState, error) {
	p, err := globalStatePath()
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return &GlobalState{Configs: make(map[string]string)}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading global state: %w", err)
	}
	var gs GlobalState
	if err := yaml.Unmarshal(data, &gs); err != nil {
		return nil, fmt.Errorf("parsing global state: %w", err)
	}
	if gs.Configs == nil {
		gs.Configs = make(map[string]string)
	}
	return &gs, nil
}

// SaveGlobalState writes the GlobalState to the global state file.
func SaveGlobalState(gs *GlobalState) error {
	p, err := globalStatePath()
	if err != nil {
		return err
	}
	data, err := yaml.Marshal(gs)
	if err != nil {
		return fmt.Errorf("marshaling global state: %w", err)
	}
	return os.WriteFile(p, data, 0o644)
}

// GetActiveConfig returns the path of the currently active config,
// or ("", nil) if none is set.
func GetActiveConfig() (string, error) {
	gs, err := LoadGlobalState()
	if err != nil {
		return "", err
	}
	return gs.Active, nil
}

// SetActiveConfig sets the active config pointer in global state.
// The path must point to an existing, readable YAML config file.
func SetActiveConfig(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if _, err := LoadFileConfig(abs); err != nil {
		return fmt.Errorf("cannot use config: %w", err)
	}
	gs, err := LoadGlobalState()
	if err != nil {
		return err
	}
	gs.Active = abs
	return SaveGlobalState(gs)
}

// SetActiveConfigByName sets the active config by registered name.
func SetActiveConfigByName(name string) error {
	gs, err := LoadGlobalState()
	if err != nil {
		return err
	}
	path, ok := gs.Configs[name]
	if !ok {
		return fmt.Errorf("no config registered with name %q", name)
	}
	gs.Active = path
	return SaveGlobalState(gs)
}

// ResetActiveConfig clears the active config pointer.
func ResetActiveConfig() error {
	gs, err := LoadGlobalState()
	if err != nil {
		return err
	}
	gs.Active = ""
	return SaveGlobalState(gs)
}

// RegisterConfig adds a name->path mapping to the global registry.
// It reads the name from the config file's name field.
func RegisterConfig(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	fc, err := LoadFileConfig(abs)
	if err != nil {
		return "", err
	}
	if fc.Name == "" {
		return "", fmt.Errorf("config file %s has no 'name' field", abs)
	}
	gs, err := LoadGlobalState()
	if err != nil {
		return "", err
	}
	gs.Configs[fc.Name] = abs
	return fc.Name, SaveGlobalState(gs)
}

// UnregisterConfig removes a name from the global registry.
// If the config was active, the active pointer is also cleared.
func UnregisterConfig(name string) error {
	gs, err := LoadGlobalState()
	if err != nil {
		return err
	}
	path, ok := gs.Configs[name]
	if !ok {
		return fmt.Errorf("no config registered with name %q", name)
	}
	delete(gs.Configs, name)
	if gs.Active == path {
		gs.Active = ""
	}
	return SaveGlobalState(gs)
}

// ListConfigs returns the global state for display purposes.
func ListConfigs() (*GlobalState, error) {
	return LoadGlobalState()
}

// LoadFileConfig reads and parses a YAML config file at the given path
func LoadFileConfig(path string) (*FileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config %s: %w", path, err)
	}
	var fc FileConfig
	if err := yaml.Unmarshal(data, &fc); err != nil {
		return nil, fmt.Errorf("parsing config %s: %w", path, err)
	}
	return &fc, nil
}

// SaveFileConfig writes a FileConfig to the given path as YAML.
func SaveFileConfig(path string, fc *FileConfig) error {
	data, err := yaml.Marshal(fc)
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}
	return os.WriteFile(path, data, 0o644)
}

// ApplyTo sets values on the Config for any fields that are present in the
// FileConfig. This should be called BEFORE FromFlags so that CLI flags override.
func (fc *FileConfig) ApplyTo(c *Config) error {
	if fc.Delimiter != "" {
		r, err := ParseDelim(fc.Delimiter)
		if err != nil {
			return fmt.Errorf("config delimiter: %w", err)
		}
		c.Delim = r
	}
	if fc.Encoding != "" {
		c.Encoding = fc.Encoding
	}
	if fc.NoHeader != nil {
		c.NoHeader = *fc.NoHeader
	}
	if fc.Quiet != nil {
		c.Quiet = *fc.Quiet
	}
	if fc.LazyQuotes != nil {
		c.LazyQuotes = *fc.LazyQuotes
	}
	if fc.SkipStart != nil {
		c.SkipStart = *fc.SkipStart
	}
	if fc.SkipEnd != nil {
		c.SkipEnd = *fc.SkipEnd
	}
	if fc.FieldsPerRecord != "" {
		n, err := parseFieldsPerRecord(fc.FieldsPerRecord)
		if err != nil {
			return fmt.Errorf("config fieldsPerRecord: %w", err)
		}
		c.FieldsPerRecord = n
	}
	if len(fc.Files) > 0 {
		c.Files = fc.Files
	}
	if len(fc.FixedColumns) > 0 {
		c.FixedColumns = fc.FixedColumns
	}
	return nil
}
