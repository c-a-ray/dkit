package core

import (
	"testing"

	"github.com/spf13/pflag"
)

func TestNewConfig(t *testing.T) {
	cfg := NewConfig()

	if cfg.Delim != ',' {
		t.Errorf("default delim = %q, want ','", cfg.Delim)
	}
	if cfg.Encoding != "utf-8-sig" {
		t.Errorf("default encoding = %q, want 'utf-8-sig'", cfg.Encoding)
	}
	if cfg.NoHeader {
		t.Error("default NoHeader should be false")
	}
	if cfg.Quiet {
		t.Error("default Quiet should be false")
	}
	if cfg.LazyQuotes {
		t.Error("default LazyQuotes should be false")
	}
}

func TestFromFlags(t *testing.T) {
	t.Run("parses all flags", func(t *testing.T) {
		fs := testFlagSet()

		// Set some non-default values
		fs.Set("delim", "tab")
		fs.Set("encoding", "latin1")
		fs.Set("noHeader", "true")
		fs.Set("quiet", "true")
		fs.Set("lazyQuotes", "true")
		fs.Set("skipStart", "2")
		fs.Set("skipEnd", "3")
		fs.Set("fieldsPerRecord", "variable")

		cfg := NewConfig()
		err := cfg.FromFlags(fs)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if cfg.Delim != '\t' {
			t.Errorf("delim = %q, want tab", cfg.Delim)
		}
		if cfg.Encoding != "latin1" {
			t.Errorf("encoding = %q, want latin1", cfg.Encoding)
		}
		if !cfg.NoHeader {
			t.Error("NoHeader should be true")
		}
		if !cfg.Quiet {
			t.Error("Quiet should be true")
		}
		if !cfg.LazyQuotes {
			t.Error("LazyQuotes should be true")
		}
		if cfg.SkipStart != 2 {
			t.Errorf("SkipStart = %d, want 2", cfg.SkipStart)
		}
		if cfg.SkipEnd != 3 {
			t.Errorf("SkipEnd = %d, want 3", cfg.SkipEnd)
		}
		if cfg.FieldsPerRecord != -1 {
			t.Errorf("FieldsPerRecord = %d, want -1", cfg.FieldsPerRecord)
		}
	})

	t.Run("invalid delimiter", func(t *testing.T) {
		fs := testFlagSet()
		fs.Set("delim", "invalid-multi-char")

		cfg := NewConfig()
		err := cfg.FromFlags(fs)
		if err == nil {
			t.Error("expected error for invalid delimiter")
		}
	})
}

func TestParseDelim(t *testing.T) {
	tests := []struct {
		input   string
		want    rune
		wantErr bool
	}{
		// Default/comma
		{"", ',', false},
		{",", ',', false},
		{"comma", ',', false},

		// Tab
		{`\t`, '\t', false},
		{"tab", '\t', false},

		// Pipe
		{"|", '|', false},
		{"pipe", '|', false},

		// Space
		{" ", ' ', false},
		{"space", ' ', false},

		// Single character
		{";", ';', false},
		{":", ':', false},

		// Invalid - multiple characters
		{"ab", 0, true},
		{"comma,tab", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseDelim(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseDelim(%q) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseDelim(%q) unexpected error: %v", tt.input, err)
				return
			}
			if got != tt.want {
				t.Errorf("ParseDelim(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestFromFlagsPreservesConfigValues(t *testing.T) {
	// Simulate: config file sets values, then FromFlags with no explicit flags should preserve them
	cfg := NewConfig()
	cfg.Delim = '|'
	cfg.Encoding = "latin1"
	cfg.LazyQuotes = true
	cfg.SkipStart = 2
	cfg.SkipEnd = 1
	cfg.FieldsPerRecord = -1

	fs := testFlagSet() // no flags changed — all at defaults

	err := cfg.FromFlags(fs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Delim != '|' {
		t.Errorf("Delim changed to %q, want '|'", cfg.Delim)
	}
	if cfg.Encoding != "latin1" {
		t.Errorf("Encoding changed to %q, want latin1", cfg.Encoding)
	}
	if !cfg.LazyQuotes {
		t.Error("LazyQuotes should still be true")
	}
	if cfg.SkipStart != 2 {
		t.Errorf("SkipStart = %d, want 2", cfg.SkipStart)
	}
	if cfg.SkipEnd != 1 {
		t.Errorf("SkipEnd = %d, want 1", cfg.SkipEnd)
	}
	if cfg.FieldsPerRecord != -1 {
		t.Errorf("FieldsPerRecord = %d, want -1", cfg.FieldsPerRecord)
	}
}

func TestFromFlagsCLIOverridesConfig(t *testing.T) {
	// Simulate: config file sets delimiter to pipe, CLI overrides to tab
	cfg := NewConfig()
	cfg.Delim = '|'
	cfg.LazyQuotes = true

	fs := testFlagSet()
	fs.Set("delim", "tab") // explicitly set — should override

	err := cfg.FromFlags(fs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Delim != '\t' {
		t.Errorf("Delim = %q, want tab (CLI should override config)", cfg.Delim)
	}
	if !cfg.LazyQuotes {
		t.Error("LazyQuotes should still be true (not overridden by CLI)")
	}
}

func testFlagSet() *pflag.FlagSet {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.String("delim", ",", "")
	fs.String("encoding", "utf-8", "")
	fs.Bool("noHeader", false, "")
	fs.Bool("quiet", false, "")
	fs.Bool("lazyQuotes", false, "")
	fs.Int("skipStart", 0, "")
	fs.Int("skipEnd", 0, "")
	fs.String("fieldsPerRecord", "", "")
	return fs
}
