package core

import (
	"io"
	"path/filepath"
	"testing"
)

func TestOpenWithEncoding(t *testing.T) {
	testFile := filepath.Join("testdata", "simple.csv")

	tests := []struct {
		name     string
		encoding string
		wantErr  bool
	}{
		{"utf-8", "utf-8", false},
		{"utf8", "utf8", false},
		{"utf-8-sig", "utf-8-sig", false},
		{"empty", "", false},
		{"latin1", "latin1", false},
		{"iso-8859-1", "iso-8859-1", false},
		{"unknown", "unknown-encoding", false}, // falls back to raw
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc, err := OpenWithEncoding(testFile, tt.encoding)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			defer rc.Close()

			// Should be able to read from it
			buf := make([]byte, 100)
			n, err := rc.Read(buf)
			if err != nil && err != io.EOF {
				t.Errorf("read error: %v", err)
			}
			if n == 0 {
				t.Error("expected to read some bytes")
			}
		})
	}

	t.Run("nonexistent file", func(t *testing.T) {
		_, err := OpenWithEncoding("nonexistent.csv", "utf-8")
		if err == nil {
			t.Error("expected error for nonexistent file")
		}
	})
}

func TestNewCSVReader(t *testing.T) {
	testFile := filepath.Join("testdata", "simple.csv")

	t.Run("reads CSV correctly", func(t *testing.T) {
		rc, err := OpenWithEncoding(testFile, "utf-8")
		if err != nil {
			t.Fatalf("open error: %v", err)
		}
		defer rc.Close()

		cr := NewCSVReader(rc, &Config{Delim: ','})

		// Read header
		hdr, err := cr.Read()
		if err != nil {
			t.Fatalf("read header error: %v", err)
		}
		if len(hdr) != 2 || hdr[0] != "Name" || hdr[1] != "Value" {
			t.Errorf("unexpected header: %v", hdr)
		}

		// Read first row
		row, err := cr.Read()
		if err != nil {
			t.Fatalf("read row error: %v", err)
		}
		if len(row) != 2 || row[0] != "Alice" || row[1] != "100" {
			t.Errorf("unexpected row: %v", row)
		}
	})

	t.Run("reads TSV with tab delimiter", func(t *testing.T) {
		tsvFile := filepath.Join("testdata", "tabs.tsv")
		rc, err := OpenWithEncoding(tsvFile, "utf-8")
		if err != nil {
			t.Fatalf("open error: %v", err)
		}
		defer rc.Close()

		cr := NewCSVReader(rc, &Config{Delim: '\t'})

		// Read header
		hdr, err := cr.Read()
		if err != nil {
			t.Fatalf("read header error: %v", err)
		}
		if len(hdr) != 2 || hdr[0] != "Name" || hdr[1] != "Value" {
			t.Errorf("unexpected header: %v", hdr)
		}
	})
}
