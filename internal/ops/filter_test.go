package ops

import (
	"testing"
)

func TestParseClause(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Clause
		wantErr bool
	}{
		{
			name:  "simple equality",
			input: "Name=Alice",
			want:  Clause{Column: "Name", Op: OpEqual, Value: "Alice"},
		},
		{
			name:  "equality with spaces",
			input: "  Name  =  Alice  ",
			want:  Clause{Column: "Name", Op: OpEqual, Value: "Alice"},
		},
		{
			name:  "not equal",
			input: "Type!=cat",
			want:  Clause{Column: "Type", Op: OpNotEqual, Value: "cat"},
		},
		{
			name:  "is empty",
			input: "Value is empty",
			want:  Clause{Column: "Value", Op: OpEmpty},
		},
		{
			name:  "is empty case insensitive",
			input: "Value IS EMPTY",
			want:  Clause{Column: "Value", Op: OpEmpty},
		},
		{
			name:  "not empty",
			input: "Value not empty",
			want:  Clause{Column: "Value", Op: OpNotEmpty},
		},
		{
			name:  "not empty case insensitive",
			input: "Value NOT EMPTY",
			want:  Clause{Column: "Value", Op: OpNotEmpty},
		},
		{
			name:  "value with equals sign",
			input: "Expr=a=b",
			want:  Clause{Column: "Expr", Op: OpEqual, Value: "a=b"},
		},
		{
			name:    "invalid - no operator",
			input:   "justtext",
			wantErr: true,
		},
		{
			name:    "invalid - empty column",
			input:   "=value",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseClause(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseClause(%q) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("parseClause(%q) unexpected error: %v", tt.input, err)
				return
			}
			if got.Column != tt.want.Column || got.Op != tt.want.Op || got.Value != tt.want.Value {
				t.Errorf("parseClause(%q) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseDisjunction(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantLen    int
		wantErr    bool
		wantFirst  Clause
		wantSecond Clause
	}{
		{
			name:      "single clause",
			input:     "A=1",
			wantLen:   1,
			wantFirst: Clause{Column: "A", Op: OpEqual, Value: "1"},
		},
		{
			name:       "two clauses OR",
			input:      "A=1|B=2",
			wantLen:    2,
			wantFirst:  Clause{Column: "A", Op: OpEqual, Value: "1"},
			wantSecond: Clause{Column: "B", Op: OpEqual, Value: "2"},
		},
		{
			name:       "three clauses with spaces",
			input:      "A=1 | B=2 | C=3",
			wantLen:    3,
			wantFirst:  Clause{Column: "A", Op: OpEqual, Value: "1"},
			wantSecond: Clause{Column: "B", Op: OpEqual, Value: "2"},
		},
		{
			name:    "empty input",
			input:   "",
			wantErr: true,
		},
		{
			name:    "only pipes",
			input:   "|||",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDisjunction(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseDisjunction(%q) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("parseDisjunction(%q) unexpected error: %v", tt.input, err)
				return
			}
			if len(got.Clauses) != tt.wantLen {
				t.Errorf("parseDisjunction(%q) got %d clauses, want %d", tt.input, len(got.Clauses), tt.wantLen)
				return
			}
			if tt.wantLen >= 1 {
				c := got.Clauses[0]
				if c.Column != tt.wantFirst.Column || c.Op != tt.wantFirst.Op || c.Value != tt.wantFirst.Value {
					t.Errorf("parseDisjunction(%q) first clause = %+v, want %+v", tt.input, c, tt.wantFirst)
				}
			}
			if tt.wantLen >= 2 {
				c := got.Clauses[1]
				if c.Column != tt.wantSecond.Column || c.Op != tt.wantSecond.Op || c.Value != tt.wantSecond.Value {
					t.Errorf("parseDisjunction(%q) second clause = %+v, want %+v", tt.input, c, tt.wantSecond)
				}
			}
		})
	}
}

func TestParseWhenFlags(t *testing.T) {
	tests := []struct {
		name      string
		input     []string
		wantLen   int
		wantErr   bool
	}{
		{
			name:    "empty",
			input:   nil,
			wantLen: 0,
		},
		{
			name:    "single flag",
			input:   []string{"A=1"},
			wantLen: 1,
		},
		{
			name:    "multiple flags (AND)",
			input:   []string{"A=1", "B=2"},
			wantLen: 2,
		},
		{
			name:    "flag with OR",
			input:   []string{"A=1|A=2"},
			wantLen: 1,
		},
		{
			name:    "invalid clause",
			input:   []string{"invalid"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseWhenFlags(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseWhenFlags(%v) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseWhenFlags(%v) unexpected error: %v", tt.input, err)
				return
			}
			if len(got.Groups) != tt.wantLen {
				t.Errorf("ParseWhenFlags(%v) got %d groups, want %d", tt.input, len(got.Groups), tt.wantLen)
			}
		})
	}
}

func TestFilterIsEmpty(t *testing.T) {
	empty := Filter{}
	if !empty.IsEmpty() {
		t.Error("empty filter should be empty")
	}

	nonEmpty := Filter{Groups: []Disjunction{{Clauses: []Clause{{Column: "A", Op: OpEqual, Value: "1"}}}}}
	if nonEmpty.IsEmpty() {
		t.Error("non-empty filter should not be empty")
	}
}

func TestFilterResolve(t *testing.T) {
	header := []string{"Name", "Type", "Status"}

	tests := []struct {
		name     string
		filter   Filter
		noHeader bool
		wantErr  bool
	}{
		{
			name:   "valid column name",
			filter: Filter{Groups: []Disjunction{{Clauses: []Clause{{Column: "Type", Op: OpEqual, Value: "cat"}}}}},
		},
		{
			name:    "invalid column name",
			filter:  Filter{Groups: []Disjunction{{Clauses: []Clause{{Column: "Unknown", Op: OpEqual, Value: "x"}}}}},
			wantErr: true,
		},
		{
			name:     "no header with numeric index",
			filter:   Filter{Groups: []Disjunction{{Clauses: []Clause{{Column: "1", Op: OpEqual, Value: "cat"}}}}},
			noHeader: true,
		},
		{
			name:     "no header with non-numeric",
			filter:   Filter{Groups: []Disjunction{{Clauses: []Clause{{Column: "Type", Op: OpEqual, Value: "cat"}}}}},
			noHeader: true,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var h []string
			if !tt.noHeader {
				h = header
			}
			_, err := tt.filter.Resolve(h, tt.noHeader)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestResolvedFilterMatch(t *testing.T) {
	tests := []struct {
		name   string
		filter ResolvedFilter
		rec    []string
		want   bool
	}{
		{
			name:   "empty filter matches all",
			filter: ResolvedFilter{},
			rec:    []string{"a", "b", "c"},
			want:   true,
		},
		{
			name: "simple equality match",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{{
				Clauses: []ResolvedClause{{Index: 0, Op: OpEqual, Value: "a"}},
			}}},
			rec:  []string{"a", "b", "c"},
			want: true,
		},
		{
			name: "simple equality no match",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{{
				Clauses: []ResolvedClause{{Index: 0, Op: OpEqual, Value: "x"}},
			}}},
			rec:  []string{"a", "b", "c"},
			want: false,
		},
		{
			name: "not equal match",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{{
				Clauses: []ResolvedClause{{Index: 0, Op: OpNotEqual, Value: "x"}},
			}}},
			rec:  []string{"a", "b", "c"},
			want: true,
		},
		{
			name: "not equal no match",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{{
				Clauses: []ResolvedClause{{Index: 0, Op: OpNotEqual, Value: "a"}},
			}}},
			rec:  []string{"a", "b", "c"},
			want: false,
		},
		{
			name: "empty check - is empty match",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{{
				Clauses: []ResolvedClause{{Index: 1, Op: OpEmpty}},
			}}},
			rec:  []string{"a", "", "c"},
			want: true,
		},
		{
			name: "empty check - is empty no match",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{{
				Clauses: []ResolvedClause{{Index: 0, Op: OpEmpty}},
			}}},
			rec:  []string{"a", "", "c"},
			want: false,
		},
		{
			name: "not empty match",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{{
				Clauses: []ResolvedClause{{Index: 0, Op: OpNotEmpty}},
			}}},
			rec:  []string{"a", "", "c"},
			want: true,
		},
		{
			name: "not empty no match",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{{
				Clauses: []ResolvedClause{{Index: 1, Op: OpNotEmpty}},
			}}},
			rec:  []string{"a", "", "c"},
			want: false,
		},
		{
			name: "OR - first matches",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{{
				Clauses: []ResolvedClause{
					{Index: 0, Op: OpEqual, Value: "a"},
					{Index: 0, Op: OpEqual, Value: "x"},
				},
			}}},
			rec:  []string{"a", "b", "c"},
			want: true,
		},
		{
			name: "OR - second matches",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{{
				Clauses: []ResolvedClause{
					{Index: 0, Op: OpEqual, Value: "x"},
					{Index: 0, Op: OpEqual, Value: "a"},
				},
			}}},
			rec:  []string{"a", "b", "c"},
			want: true,
		},
		{
			name: "OR - none matches",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{{
				Clauses: []ResolvedClause{
					{Index: 0, Op: OpEqual, Value: "x"},
					{Index: 0, Op: OpEqual, Value: "y"},
				},
			}}},
			rec:  []string{"a", "b", "c"},
			want: false,
		},
		{
			name: "AND - both match",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{
				{Clauses: []ResolvedClause{{Index: 0, Op: OpEqual, Value: "a"}}},
				{Clauses: []ResolvedClause{{Index: 1, Op: OpEqual, Value: "b"}}},
			}},
			rec:  []string{"a", "b", "c"},
			want: true,
		},
		{
			name: "AND - first fails",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{
				{Clauses: []ResolvedClause{{Index: 0, Op: OpEqual, Value: "x"}}},
				{Clauses: []ResolvedClause{{Index: 1, Op: OpEqual, Value: "b"}}},
			}},
			rec:  []string{"a", "b", "c"},
			want: false,
		},
		{
			name: "AND - second fails",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{
				{Clauses: []ResolvedClause{{Index: 0, Op: OpEqual, Value: "a"}}},
				{Clauses: []ResolvedClause{{Index: 1, Op: OpEqual, Value: "x"}}},
			}},
			rec:  []string{"a", "b", "c"},
			want: false,
		},
		{
			name: "out of bounds index treated as empty",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{{
				Clauses: []ResolvedClause{{Index: 10, Op: OpEmpty}},
			}}},
			rec:  []string{"a", "b"},
			want: true,
		},
		{
			name: "whitespace trimmed",
			filter: ResolvedFilter{Groups: []ResolvedDisjunction{{
				Clauses: []ResolvedClause{{Index: 0, Op: OpEqual, Value: "a"}},
			}}},
			rec:  []string{"  a  ", "b"},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.filter.Match(tt.rec)
			if got != tt.want {
				t.Errorf("Match(%v) = %v, want %v", tt.rec, got, tt.want)
			}
		})
	}
}
