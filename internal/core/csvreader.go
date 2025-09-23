package core

import (
	"bufio"
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

// NewCSVReader returns a csv.Reader configured with the given delimiter and options
func NewCSVReader(r io.Reader, delim rune, lazy bool) *csv.Reader {
	cr := csv.NewReader(r)
	cr.Comma = delim
	cr.LazyQuotes = lazy
	cr.ReuseRecord = true
	return cr
}
