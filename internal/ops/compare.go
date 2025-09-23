package ops

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/c-a-ray/dkit/internal/core"
)

// CompareOpts configures how two columns are compared across CSV files
// ColA and ColB may be header names or index strings
type CompareOpts struct {
	ColA       string
	ColB       string
	IgnoreCase bool
	AllowEmpty bool
	Quiet      bool
	Config     *core.Config
}

// CompareResult summarizes the outcome of a column comparison
type CompareResult struct {
	FilesScanned int
	RowsSeen     int
	Mismatches   int
}

// CompareColumns compares two columns across one or more files
func CompareColumns(files []string, o CompareOpts) (CompareResult, error) {
	if len(files) == 0 {
		return CompareResult{}, errors.New("no files")
	}

	res := CompareResult{}

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
					fmt.Printf("%s line %d\n  A: %s\n  B: %s\n", filepathBase(path), line, rawA, rawB)
				}
			}

			line++
		}

		rc.Close()
		res.FilesScanned++
	}

	fmt.Fprintf(os.Stderr, "\nScanned %d files, %d rows. Mismatches: %d\n", res.FilesScanned, res.RowsSeen, res.Mismatches)

	return res, nil
}
