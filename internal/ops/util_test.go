package ops

import (
	"testing"
)

func TestParseIndex(t *testing.T) {
	tests := []struct {
		input   string
		want    int
		wantErr bool
	}{
		{"0", 0, false},
		{"1", 1, false},
		{"10", 10, false},
		{"-1", 0, true},
		{"abc", 0, true},
		{"", 0, true},
		{"1.5", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseIndex(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseIndex(%q) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("parseIndex(%q) unexpected error: %v", tt.input, err)
				return
			}
			if got != tt.want {
				t.Errorf("parseIndex(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestResolveHeaderIndex(t *testing.T) {
	header := []string{"Name", "Type", "Status"}

	tests := []struct {
		name    string
		want    int
		wantErr bool
	}{
		{"Name", 0, false},
		{"Type", 1, false},
		{"Status", 2, false},
		{"Unknown", 0, true},
		{"name", 0, true}, // case sensitive
		{"", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveHeaderIndex(header, tt.name)
			if tt.wantErr {
				if err == nil {
					t.Errorf("resolveHeaderIndex(%q) expected error, got nil", tt.name)
				}
				return
			}
			if err != nil {
				t.Errorf("resolveHeaderIndex(%q) unexpected error: %v", tt.name, err)
				return
			}
			if got != tt.want {
				t.Errorf("resolveHeaderIndex(%q) = %d, want %d", tt.name, got, tt.want)
			}
		})
	}
}

func TestFilepathBase(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"/path/to/file.csv", "file.csv"},
		{"file.csv", "file.csv"},
		{"/file.csv", "file.csv"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := filepathBase(tt.input)
			if got != tt.want {
				t.Errorf("filepathBase(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestJoinKV(t *testing.T) {
	tests := []struct {
		names []string
		vals  []string
		want  string
	}{
		{[]string{"A", "B"}, []string{"1", "2"}, `A="1", B="2"`},
		{[]string{"Name"}, []string{"Alice"}, `Name="Alice"`},
		{[]string{"A", "B", "C"}, []string{"1", "2"}, `A="1", B="2"`}, // truncated to min length
		{nil, nil, ""},
		{[]string{}, []string{}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := joinKV(tt.names, tt.vals)
			if got != tt.want {
				t.Errorf("joinKV(%v, %v) = %q, want %q", tt.names, tt.vals, got, tt.want)
			}
		})
	}
}
