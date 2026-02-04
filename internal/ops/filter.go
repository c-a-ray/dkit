package ops

import (
	"fmt"
	"strings"
)

// FilterOp represents the comparison operator for a filter clause
type FilterOp int

const (
	OpEqual    FilterOp = iota // =
	OpNotEqual                 // !=
	OpEmpty                    // is empty
	OpNotEmpty                 // not empty
)

// Clause represents a single condition: Column Op Value
type Clause struct {
	Column string
	Op     FilterOp
	Value  string // unused for OpEmpty/OpNotEmpty
}

// ResolvedClause is a Clause with column index resolved
type ResolvedClause struct {
	Index int
	Op    FilterOp
	Value string
}

// Disjunction is a group of clauses ORed together (from one --when flag)
type Disjunction struct {
	Clauses []Clause
}

// ResolvedDisjunction is a Disjunction with indices resolved
type ResolvedDisjunction struct {
	Clauses []ResolvedClause
}

// Filter represents a complete filter expression (CNF: conjunction of disjunctions)
// Each Disjunction is ANDed together
type Filter struct {
	Groups []Disjunction
}

// ResolvedFilter is ready to evaluate against rows
type ResolvedFilter struct {
	Groups []ResolvedDisjunction
}

// ParseWhenFlags parses multiple --when flag values into a Filter
func ParseWhenFlags(whens []string) (Filter, error) {
	f := Filter{}
	for _, w := range whens {
		disj, err := parseDisjunction(w)
		if err != nil {
			return Filter{}, err
		}
		f.Groups = append(f.Groups, disj)
	}
	return f, nil
}

// parseDisjunction parses a single --when value which may contain "|" for OR
func parseDisjunction(s string) (Disjunction, error) {
	d := Disjunction{}
	parts := strings.Split(s, "|")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		c, err := parseClause(p)
		if err != nil {
			return Disjunction{}, err
		}
		d.Clauses = append(d.Clauses, c)
	}
	if len(d.Clauses) == 0 {
		return Disjunction{}, fmt.Errorf("empty condition")
	}
	return d, nil
}

// parseClause parses a single condition like "B=dog", "B!=cat", "B is empty", "B not empty"
func parseClause(s string) (Clause, error) {
	lower := strings.ToLower(s)

	// Check for "is empty" (case insensitive)
	if idx := strings.Index(lower, " is empty"); idx > 0 {
		col := strings.TrimSpace(s[:idx])
		return Clause{Column: col, Op: OpEmpty}, nil
	}

	// Check for "not empty" (case insensitive)
	if idx := strings.Index(lower, " not empty"); idx > 0 {
		col := strings.TrimSpace(s[:idx])
		return Clause{Column: col, Op: OpNotEmpty}, nil
	}

	// Check for != before = (order matters)
	if idx := strings.Index(s, "!="); idx > 0 {
		col := strings.TrimSpace(s[:idx])
		val := strings.TrimSpace(s[idx+2:])
		return Clause{Column: col, Op: OpNotEqual, Value: val}, nil
	}

	// Check for =
	if idx := strings.Index(s, "="); idx > 0 {
		col := strings.TrimSpace(s[:idx])
		val := strings.TrimSpace(s[idx+1:])
		return Clause{Column: col, Op: OpEqual, Value: val}, nil
	}

	return Clause{}, fmt.Errorf("invalid condition %q (expected COL=VAL, COL!=VAL, COL is empty, or COL not empty)", s)
}

// IsEmpty returns true if the filter has no conditions
func (f Filter) IsEmpty() bool {
	return len(f.Groups) == 0
}

// Resolve resolves column names to indices using the header
func (f Filter) Resolve(header []string, noHeader bool) (ResolvedFilter, error) {
	rf := ResolvedFilter{}
	for _, disj := range f.Groups {
		rd := ResolvedDisjunction{}
		for _, c := range disj.Clauses {
			var idx int
			var err error

			if noHeader {
				idx, err = parseIndex(c.Column)
				if err != nil {
					return ResolvedFilter{}, fmt.Errorf("with --no-header, filter column must be numeric index: %q", c.Column)
				}
			} else {
				idx, err = resolveHeaderIndex(header, c.Column)
				if err != nil {
					return ResolvedFilter{}, fmt.Errorf("filter column %q not found in header", c.Column)
				}
			}

			rd.Clauses = append(rd.Clauses, ResolvedClause{
				Index: idx,
				Op:    c.Op,
				Value: c.Value,
			})
		}
		rf.Groups = append(rf.Groups, rd)
	}
	return rf, nil
}

// Match evaluates the filter against a record
// Returns true if the record matches all conditions (CNF: all groups must pass)
func (rf ResolvedFilter) Match(rec []string) bool {
	if len(rf.Groups) == 0 {
		return true
	}

	// CNF: all disjunctions must be true (AND)
	for _, disj := range rf.Groups {
		if !disj.match(rec) {
			return false
		}
	}
	return true
}

// match returns true if at least one clause matches (OR)
func (rd ResolvedDisjunction) match(rec []string) bool {
	for _, c := range rd.Clauses {
		if c.match(rec) {
			return true
		}
	}
	return false
}

// match evaluates a single clause against the record
func (rc ResolvedClause) match(rec []string) bool {
	// Handle out-of-bounds: treat as empty string
	var val string
	if rc.Index < len(rec) {
		val = strings.TrimSpace(rec[rc.Index])
	}

	switch rc.Op {
	case OpEqual:
		return val == rc.Value
	case OpNotEqual:
		return val != rc.Value
	case OpEmpty:
		return val == ""
	case OpNotEmpty:
		return val != ""
	default:
		return false
	}
}
