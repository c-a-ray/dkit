package ops

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/c-a-ray/dkit/internal/core"
)

// OutputFormat specifies how comparison results are displayed
type OutputFormat int

const (
	OutputLines OutputFormat = iota // default: file + line + values
	OutputTable                     // side-by-side values only
	OutputCSV                       // CSV with values, file, line
)

// CompareOpts configures how two columns are compared across CSV files
// ColA and TargetCols may be header names or index strings
type CompareOpts struct {
	ColA       string
	TargetCols []string // columns to compare against (OR logic: A must match at least one)
	IgnoreCase bool
	AllowEmpty bool
	Quiet      bool
	Format     OutputFormat
	CSVPath    string // output path when Format == OutputCSV
	Config     *core.Config
}

// CompareResult summarizes the outcome of a column comparison
type CompareResult struct {
	FilesScanned int
	RowsSeen     int
	Mismatches   int
}

// mismatch holds data about a single mismatching row
type mismatch struct {
	file       string
	line       int
	valA       string
	targetVals []string // values from all target columns
}

// CompareColumns compares ColA against TargetCols across one or more files.
// A row is a match if ColA equals ANY of the target columns (OR logic).
func CompareColumns(files []string, o CompareOpts) (CompareResult, error) {
	if len(files) == 0 {
		return CompareResult{}, errors.New("no files")
	}
	if len(o.TargetCols) == 0 {
		return CompareResult{}, errors.New("no target columns specified")
	}

	res := CompareResult{}
	var mismatches []mismatch

	for _, path := range files {
		rc, err := core.OpenWithEncoding(path, o.Config.Encoding)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] cannot read %s: %v\n", path, err)
			continue
		}

		r := core.SkipLines(rc, o.Config.SkipStart, o.Config.SkipEnd)
		cr := core.NewCSVReader(r, o.Config.Delim, o.Config.LazyQuotes)
		var iA int
		targetIdxs := make([]int, len(o.TargetCols))

		if o.Config.NoHeader {
			ia, err := parseIndex(o.ColA)
			if err != nil {
				rc.Close()
				return res, fmt.Errorf("--no-header: col A must be index: %w", err)
			}
			iA = ia

			for i, col := range o.TargetCols {
				idx, err := parseIndex(col)
				if err != nil {
					rc.Close()
					return res, fmt.Errorf("--no-header: target col %q must be index: %w", col, err)
				}
				targetIdxs[i] = idx
			}
		} else {
			hdr, err := cr.Read()
			if err == io.EOF {
				fmt.Fprintf(os.Stderr, "[WARN] %s is empty\n", path)
				rc.Close()
				continue
			} else if err != nil {
				fmt.Fprintf(os.Stderr, "[WARN] %s: %v\n", path, err)
				rc.Close()
				continue
			}

			iA, err = resolveHeaderIndex(hdr, o.ColA)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[WARN] %s: %v\n", path, err)
				rc.Close()
				continue
			}

			skip := false
			for i, col := range o.TargetCols {
				idx, err := resolveHeaderIndex(hdr, col)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[WARN] %s: %v\n", path, err)
					skip = true
					break
				}
				targetIdxs[i] = idx
			}
			if skip {
				rc.Close()
				continue
			}
		}

		line := 1
		if !o.Config.NoHeader {
			line = 2
		}

		for {
			rec, err := cr.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "[WARN] %s: %v\n", path, err)
				break
			}
			res.RowsSeen++

			// Check if any index is out of bounds
			if iA >= len(rec) {
				line++
				continue
			}
			outOfBounds := false
			for _, idx := range targetIdxs {
				if idx >= len(rec) {
					outOfBounds = true
					break
				}
			}
			if outOfBounds {
				line++
				continue
			}

			rawA := strings.TrimSpace(rec[iA])
			a := rawA
			if o.IgnoreCase {
				a = strings.ToLower(a)
			}

			// Collect target values and check for match
			rawTargets := make([]string, len(targetIdxs))
			matched := false
			allTargetsEmpty := true

			for i, idx := range targetIdxs {
				rawTargets[i] = strings.TrimSpace(rec[idx])
				if rawTargets[i] != "" {
					allTargetsEmpty = false
				}

				target := rawTargets[i]
				if o.IgnoreCase {
					target = strings.ToLower(target)
				}
				if a == target {
					matched = true
				}
			}

			// Skip if A is empty or all targets are empty (unless AllowEmpty)
			if !o.AllowEmpty && (a == "" || allTargetsEmpty) {
				line++
				continue
			}

			if !matched {
				res.Mismatches++
				if !o.Quiet {
					mismatches = append(mismatches, mismatch{
						file:       filepathBase(path),
						line:       line,
						valA:       rawA,
						targetVals: rawTargets,
					})
				}
			}

			line++
		}

		rc.Close()
		res.FilesScanned++
	}

	// Output mismatches in the requested format
	if !o.Quiet && len(mismatches) > 0 {
		if err := outputMismatches(mismatches, o); err != nil {
			return res, err
		}
	}

	fmt.Fprintf(os.Stderr, "\nScanned %d files, %d rows. Mismatches: %d\n", res.FilesScanned, res.RowsSeen, res.Mismatches)

	return res, nil
}

// outputMismatches writes mismatches in the requested format
func outputMismatches(mismatches []mismatch, o CompareOpts) error {
	switch o.Format {
	case OutputTable:
		return outputTable(mismatches, o)
	case OutputCSV:
		return outputCSV(mismatches, o)
	default: // OutputLines
		return outputLines(mismatches, o)
	}
}

// outputLines prints the default verbose format
func outputLines(mismatches []mismatch, o CompareOpts) error {
	for _, m := range mismatches {
		fmt.Printf("%s line %d\n  %s: %s\n", m.file, m.line, o.ColA, m.valA)
		for i, col := range o.TargetCols {
			fmt.Printf("  %s: %s\n", col, m.targetVals[i])
		}
	}
	return nil
}

// outputTable prints a side-by-side table without file/line info
func outputTable(mismatches []mismatch, o CompareOpts) error {
	// Find max width for each column
	maxWidths := make([]int, 1+len(o.TargetCols))
	maxWidths[0] = len(o.ColA)
	for i, col := range o.TargetCols {
		maxWidths[i+1] = len(col)
	}

	for _, m := range mismatches {
		if len(m.valA) > maxWidths[0] {
			maxWidths[0] = len(m.valA)
		}
		for i, val := range m.targetVals {
			if len(val) > maxWidths[i+1] {
				maxWidths[i+1] = len(val)
			}
		}
	}

	// Print header
	fmt.Printf("%-*s", maxWidths[0], o.ColA)
	for i, col := range o.TargetCols {
		fmt.Printf(" | %-*s", maxWidths[i+1], col)
	}
	fmt.Println()

	// Print separator
	fmt.Print(strings.Repeat("-", maxWidths[0]))
	for i := range o.TargetCols {
		fmt.Printf("-+-%s", strings.Repeat("-", maxWidths[i+1]))
	}
	fmt.Println()

	// Print rows
	for _, m := range mismatches {
		fmt.Printf("%-*s", maxWidths[0], m.valA)
		for i, val := range m.targetVals {
			fmt.Printf(" | %-*s", maxWidths[i+1], val)
		}
		fmt.Println()
	}
	return nil
}

// outputCSV writes mismatches to a CSV file
func outputCSV(mismatches []mismatch, o CompareOpts) error {
	f, err := os.Create(o.CSVPath)
	if err != nil {
		return fmt.Errorf("cannot create %s: %w", o.CSVPath, err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Write header: ColA, target cols..., file, line
	header := make([]string, 0, 2+len(o.TargetCols)+2)
	header = append(header, o.ColA)
	header = append(header, o.TargetCols...)
	header = append(header, "file", "line")
	if err := w.Write(header); err != nil {
		return err
	}

	// Write rows
	for _, m := range mismatches {
		row := make([]string, 0, 2+len(m.targetVals)+2)
		row = append(row, m.valA)
		row = append(row, m.targetVals...)
		row = append(row, m.file, strconv.Itoa(m.line))
		if err := w.Write(row); err != nil {
			return err
		}
	}

	fmt.Fprintf(os.Stderr, "Wrote %d mismatches to %s\n", len(mismatches), o.CSVPath)
	return nil
}
