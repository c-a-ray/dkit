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

// NewCSVReader returns a csv.Reader configured from the given Config
func NewCSVReader(r io.Reader, cfg *Config) *csv.Reader {
	cr := csv.NewReader(r)
	cr.Comma = cfg.Delim
	cr.LazyQuotes = cfg.LazyQuotes
	cr.FieldsPerRecord = cfg.FieldsPerRecord
	cr.ReuseRecord = true
	return cr
}
