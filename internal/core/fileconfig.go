package core

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// DotfileName is the name of the pointer file placed in the working directory
const DotfileName = ".dkit"

// FileConfig represents the contents of a dkit YAML configuration file
type FileConfig struct {
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

// LoadDotfile reads the .dkit file from the given directory and returns the
// path to the YAML config it references. Returns ("", nil) if no .dkit exists.
func LoadDotfile(dir string) (string, error) {
	dotpath := filepath.Join(dir, DotfileName)
	data, err := os.ReadFile(dotpath)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", dotpath, err)
	}
	cfgPath := strings.TrimSpace(string(data))
	if cfgPath == "" {
		return "", fmt.Errorf("%s is empty", dotpath)
	}
	if !filepath.IsAbs(cfgPath) {
		cfgPath = filepath.Join(dir, cfgPath)
	}
	return cfgPath, nil
}

// WriteDotfile writes the config file path to .dkit in the given directory
func WriteDotfile(dir, cfgPath string) error {
	dotpath := filepath.Join(dir, DotfileName)
	return os.WriteFile(dotpath, []byte(cfgPath+"\n"), 0o644)
}

// RemoveDotfile removes the .dkit file from the given directory
func RemoveDotfile(dir string) error {
	dotpath := filepath.Join(dir, DotfileName)
	err := os.Remove(dotpath)
	if os.IsNotExist(err) {
		return nil
	}
	return err
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
