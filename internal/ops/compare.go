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
// ColA and ColB may be header names or index strings
type CompareOpts struct {
	ColA       string
	ColB       string
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
	file string
	line int
	valA string
	valB string
}

// CompareColumns compares two columns across one or more files
func CompareColumns(files []string, o CompareOpts) (CompareResult, error) {
	if len(files) == 0 {
		return CompareResult{}, errors.New("no files")
	}

	res := CompareResult{}
	var mismatches []mismatch

	for _, path := range files {
		rc, err := core.OpenWithEncoding(path, o.Config.Encoding)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] cannot read %s: %v\n", path, err)
			continue
		}

		cr := core.NewCSVReader(rc, o.Config.Delim, o.Config.LazyQuotes)
		var iA, iB int

		if o.Config.NoHeader {
			ia, err := parseIndex(o.ColA)
			if err != nil {
				rc.Close()
				return res, fmt.Errorf("--no-header: col A must be index: %w", err)
			}

			ib, err := parseIndex(o.ColB)
			if err != nil {
				rc.Close()
				return res, fmt.Errorf("--no-header: col B must be index: %w", err)
			}

			iA, iB = ia, ib
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

			iB, err = resolveHeaderIndex(hdr, o.ColB)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[WARN] %s: %v\n", path, err)
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

			if iA >= len(rec) || iB >= len(rec) {
				line++
				continue
			}

			rawA, rawB := strings.TrimSpace(rec[iA]), strings.TrimSpace(rec[iB])
			a, b := rawA, rawB

			if o.IgnoreCase {
				a = strings.ToLower(a)
				b = strings.ToLower(b)
			}

			if !o.AllowEmpty && (a == "" || b == "") {
				line++
				continue
			}

			if a != b {
				res.Mismatches++
				if !o.Quiet {
					mismatches = append(mismatches, mismatch{
						file: filepathBase(path),
						line: line,
						valA: rawA,
						valB: rawB,
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
		return outputLines(mismatches)
	}
}

// outputLines prints the default verbose format
func outputLines(mismatches []mismatch) error {
	for _, m := range mismatches {
		fmt.Printf("%s line %d\n  A: %s\n  B: %s\n", m.file, m.line, m.valA, m.valB)
	}
	return nil
}

// outputTable prints a side-by-side table without file/line info
func outputTable(mismatches []mismatch, o CompareOpts) error {
	// Find max width for column A
	maxA := len(o.ColA)
	for _, m := range mismatches {
		if len(m.valA) > maxA {
			maxA = len(m.valA)
		}
	}

	// Print header
	fmt.Printf("%-*s | %s\n", maxA, o.ColA, o.ColB)
	fmt.Printf("%s-+-%s\n", strings.Repeat("-", maxA), strings.Repeat("-", maxA))

	// Print rows
	for _, m := range mismatches {
		fmt.Printf("%-*s | %s\n", maxA, m.valA, m.valB)
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

	// Write header
	if err := w.Write([]string{o.ColA, o.ColB, "file", "line"}); err != nil {
		return err
	}

	// Write rows
	for _, m := range mismatches {
		if err := w.Write([]string{m.valA, m.valB, m.file, strconv.Itoa(m.line)}); err != nil {
			return err
		}
	}

	fmt.Fprintf(os.Stderr, "Wrote %d mismatches to %s\n", len(mismatches), o.CSVPath)
	return nil
}
