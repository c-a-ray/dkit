package core

import (
	"io"
	"path/filepath"
	"strings"
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

func TestFixedWidthReader(t *testing.T) {
	input := "001 Alice     100\n002 Bob       200\n003 Carol     300\n"
	columns := []FixedColumnDef{
		{Name: "ID", Start: 0, End: 3},
		{Name: "NAME", Start: 4, End: 14},
		{Name: "VALUE", Start: 14, End: 17},
	}

	rr := newFixedWidthReader(strings.NewReader(input), columns)

	// First read should return synthetic header
	hdr, err := rr.Read()
	if err != nil {
		t.Fatalf("read header error: %v", err)
	}
	if len(hdr) != 3 || hdr[0] != "ID" || hdr[1] != "NAME" || hdr[2] != "VALUE" {
		t.Errorf("unexpected header: %v", hdr)
	}

	// Read data rows
	row, err := rr.Read()
	if err != nil {
		t.Fatalf("read row 1 error: %v", err)
	}
	if row[0] != "001" || row[1] != "Alice" || row[2] != "100" {
		t.Errorf("unexpected row 1: %v", row)
	}

	row, err = rr.Read()
	if err != nil {
		t.Fatalf("read row 2 error: %v", err)
	}
	if row[0] != "002" || row[1] != "Bob" || row[2] != "200" {
		t.Errorf("unexpected row 2: %v", row)
	}

	row, err = rr.Read()
	if err != nil {
		t.Fatalf("read row 3 error: %v", err)
	}
	if row[0] != "003" || row[1] != "Carol" || row[2] != "300" {
		t.Errorf("unexpected row 3: %v", row)
	}

	// EOF
	_, err = rr.Read()
	if err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestNewRecordReader(t *testing.T) {
	t.Run("returns CSV reader when no fixed columns", func(t *testing.T) {
		cfg := &Config{Delim: ','}
		rr := NewRecordReader(strings.NewReader("a,b\n1,2\n"), cfg)
		hdr, err := rr.Read()
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if hdr[0] != "a" || hdr[1] != "b" {
			t.Errorf("unexpected header: %v", hdr)
		}
	})

	t.Run("returns fixed-width reader when columns defined", func(t *testing.T) {
		cfg := &Config{
			FixedColumns: []FixedColumnDef{
				{Name: "X", Start: 0, End: 3},
				{Name: "Y", Start: 3, End: 6},
			},
		}
		rr := NewRecordReader(strings.NewReader("aaabbb\ncccddd\n"), cfg)
		hdr, err := rr.Read()
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if hdr[0] != "X" || hdr[1] != "Y" {
			t.Errorf("unexpected header: %v", hdr)
		}
		row, err := rr.Read()
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if row[0] != "aaa" || row[1] != "bbb" {
			t.Errorf("unexpected row: %v", row)
		}
	})
}
