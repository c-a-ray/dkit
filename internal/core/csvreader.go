package core

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"io"
	"os"
	"strings"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

// OpenWithEncoding opens a file at the given path and wraps it with a decoder if the specified encoding requires one
func OpenWithEncoding(path string, enc string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(enc) {
	case "", "utf-8", "utf8", "utf-8-sig":
		return f, nil
	case "latin1", "iso-8859-1":
		rc := struct {
			io.Reader
			io.Closer
		}{
			Reader: transform.NewReader(bufio.NewReader(f), charmap.ISO8859_1.NewDecoder()),
			Closer: f,
		}
		return rc, nil
	default:
		// Fallback: return raw and let ops decide; can add more encodings later
		return f, nil
	}
}

// SkipLines returns a reader that omits the first start lines and last end lines
// from the underlying reader. If both are zero it returns r unchanged.
func SkipLines(r io.Reader, start, end int) io.Reader {
	if start == 0 && end == 0 {
		return r
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return bytes.NewReader(data)
	}

	lines := bytes.SplitAfter(data, []byte("\n"))
	// Remove trailing empty element produced when input ends with newline
	if len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}

	if start > 0 {
		if start >= len(lines) {
			return strings.NewReader("")
		}
		lines = lines[start:]
	}

	if end > 0 {
		if end >= len(lines) {
			return strings.NewReader("")
		}
		lines = lines[:len(lines)-end]
	}

	return bytes.NewReader(bytes.Join(lines, nil))
}

// RecordReader is the interface for reading tabular records (CSV or fixed-width)
type RecordReader interface {
	Read() ([]string, error)
}

// NewCSVReader returns a csv.Reader configured from the given Config
func NewCSVReader(r io.Reader, cfg *Config) *csv.Reader {
	cr := csv.NewReader(r)
	cr.Comma = cfg.Delim
	cr.LazyQuotes = cfg.LazyQuotes
	cr.FieldsPerRecord = cfg.FieldsPerRecord
	cr.ReuseRecord = true
	return cr
}

// NewRecordReader returns a RecordReader appropriate for the config.
// If FixedColumns is defined, returns a FixedWidthReader that emits
// column names as a synthetic header row followed by extracted values.
// Otherwise returns a standard CSV reader.
func NewRecordReader(r io.Reader, cfg *Config) RecordReader {
	if len(cfg.FixedColumns) > 0 {
		return newFixedWidthReader(r, cfg.FixedColumns)
	}
	return NewCSVReader(r, cfg)
}

// FixedWidthReader reads fixed-width lines and extracts named columns.
// The first Read() returns column names as a synthetic header row.
type FixedWidthReader struct {
	scanner    *bufio.Scanner
	columns    []FixedColumnDef
	headerSent bool
}

func newFixedWidthReader(r io.Reader, columns []FixedColumnDef) *FixedWidthReader {
	return &FixedWidthReader{
		scanner: bufio.NewScanner(r),
		columns: columns,
	}
}

func (fw *FixedWidthReader) Read() ([]string, error) {
	if !fw.headerSent {
		fw.headerSent = true
		hdr := make([]string, len(fw.columns))
		for i, col := range fw.columns {
			hdr[i] = col.Name
		}
		return hdr, nil
	}

	if !fw.scanner.Scan() {
		if err := fw.scanner.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	line := fw.scanner.Text()
	rec := make([]string, len(fw.columns))
	for i, col := range fw.columns {
		if col.Start >= len(line) {
			rec[i] = ""
			continue
		}
		end := min(col.End, len(line))
		rec[i] = strings.TrimSpace(line[col.Start:end])
	}
	return rec, nil
}
