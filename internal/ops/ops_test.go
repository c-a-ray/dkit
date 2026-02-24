package ops

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/c-a-ray/dkit/internal/core"
)

// testConfig returns a default Config for testing
func testConfig() *core.Config {
	return &core.Config{
		Delim:      ',',
		Encoding:   "utf-8",
		NoHeader:   false,
		LazyQuotes: false,
		Quiet:      false,
	}
}

// testdataPath returns the path to a testdata file
func testdataPath(name string) string {
	return filepath.Join("testdata", name)
}

// captureStdout captures stdout during fn execution and returns the output
func captureStdout(fn func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	return buf.String()
}

// captureStderr captures stderr during fn execution and returns the output
func captureStderr(fn func()) string {
	old := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	fn()

	w.Close()
	os.Stderr = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	return buf.String()
}

// TestCompareColumns tests the CompareColumns function
func TestCompareColumns(t *testing.T) {
	cfg := testConfig()
	cfg.Quiet = true

	tests := []struct {
		name           string
		files          []string
		opts           CompareOpts
		wantMismatches int
		wantErr        bool
	}{
		{
			name:  "matching columns",
			files: []string{testdataPath("compare.csv")},
			opts: CompareOpts{
				ColA:       "ColA",
				TargetCols: []string{"ColB"},
				Config:     cfg,
			},
			wantMismatches: 2, // cherry/Cherry and date/fig
		},
		{
			name:  "matching columns ignore case",
			files: []string{testdataPath("compare.csv")},
			opts: CompareOpts{
				ColA:       "ColA",
				TargetCols: []string{"ColB"},
				IgnoreCase: true,
				Config:     cfg,
			},
			wantMismatches: 1, // only date/fig
		},
		{
			name:  "identical columns",
			files: []string{testdataPath("compare.csv")},
			opts: CompareOpts{
				ColA:       "ColA",
				TargetCols: []string{"ColA"},
				Config:     cfg,
			},
			wantMismatches: 0,
		},
		{
			name:    "no files",
			files:   []string{},
			opts:    CompareOpts{ColA: "A", TargetCols: []string{"B"}, Config: cfg},
			wantErr: true,
		},
		{
			name:  "multiple targets OR logic",
			files: []string{testdataPath("compare.csv")},
			opts: CompareOpts{
				ColA:       "ColA",
				TargetCols: []string{"ColB", "ColC"},
				Config:     cfg,
			},
			wantMismatches: 0, // cherry matches ColC, date matches ColC
		},
		{
			name:  "multiple targets with one mismatch",
			files: []string{testdataPath("compare.csv")},
			opts: CompareOpts{
				ColA:       "ColC",
				TargetCols: []string{"ColA", "ColB"},
				Config:     cfg,
			},
			wantMismatches: 1, // APPLE doesn't match apple (case sensitive)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.opts.Quiet = true
			// Capture stderr to suppress summary output
			captureStderr(func() {
				res, err := CompareColumns(tt.files, tt.opts)
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
				if res.Mismatches != tt.wantMismatches {
					t.Errorf("got %d mismatches, want %d", res.Mismatches, tt.wantMismatches)
				}
			})
		})
	}
}

// TestColumnValues tests the ColumnValues function
func TestColumnValues(t *testing.T) {
	cfg := testConfig()

	t.Run("unique values", func(t *testing.T) {
		output := captureStdout(func() {
			err := ColumnValues([]string{testdataPath("basic.csv")}, ValsOpts{
				Column: "Type",
				Mode:   ValsUniq,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if !strings.Contains(output, "cat") || !strings.Contains(output, "dog") {
			t.Errorf("expected cat and dog in output, got: %s", output)
		}
	})

	t.Run("frequency counts", func(t *testing.T) {
		output := captureStdout(func() {
			err := ColumnValues([]string{testdataPath("basic.csv")}, ValsOpts{
				Column: "Type",
				Mode:   ValsFreq,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		// Each type appears twice
		if !strings.Contains(output, "2") {
			t.Errorf("expected frequency count of 2 in output, got: %s", output)
		}
	})

	t.Run("with filter equality", func(t *testing.T) {
		filter, _ := ParseWhenFlags([]string{"Type=cat"})
		output := captureStdout(func() {
			err := ColumnValues([]string{testdataPath("basic.csv")}, ValsOpts{
				Column: "Name",
				Mode:   ValsUniq,
				Filter: filter,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if !strings.Contains(output, "Alice") || !strings.Contains(output, "Carol") {
			t.Errorf("expected Alice and Carol (cats), got: %s", output)
		}
		if strings.Contains(output, "Bob") || strings.Contains(output, "Dave") {
			t.Errorf("should not contain Bob or Dave (dogs), got: %s", output)
		}
	})

	t.Run("with filter AND", func(t *testing.T) {
		filter, _ := ParseWhenFlags([]string{"Type=cat", "Status=active"})
		output := captureStdout(func() {
			err := ColumnValues([]string{testdataPath("basic.csv")}, ValsOpts{
				Column: "Name",
				Mode:   ValsUniq,
				Filter: filter,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if !strings.Contains(output, "Alice") {
			t.Errorf("expected Alice (active cat), got: %s", output)
		}
		if strings.Contains(output, "Carol") {
			t.Errorf("should not contain Carol (inactive cat), got: %s", output)
		}
	})

	t.Run("with filter OR", func(t *testing.T) {
		filter, _ := ParseWhenFlags([]string{"Type=cat|Status=inactive"})
		output := captureStdout(func() {
			err := ColumnValues([]string{testdataPath("basic.csv")}, ValsOpts{
				Column: "Name",
				Mode:   ValsUniq,
				Filter: filter,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		// Alice (cat), Carol (cat+inactive), Dave (inactive)
		if !strings.Contains(output, "Alice") || !strings.Contains(output, "Carol") || !strings.Contains(output, "Dave") {
			t.Errorf("expected Alice, Carol, Dave, got: %s", output)
		}
		// Bob is active dog - doesn't match
		if strings.Contains(output, "Bob") {
			t.Errorf("should not contain Bob, got: %s", output)
		}
	})

	t.Run("with filter not empty", func(t *testing.T) {
		filter, _ := ParseWhenFlags([]string{"Owner not empty"})
		output := captureStdout(func() {
			err := ColumnValues([]string{testdataPath("animals.csv")}, ValsOpts{
				Column: "Name",
				Mode:   ValsUniq,
				Filter: filter,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		// Buddy has no owner
		if strings.Contains(output, "Buddy") {
			t.Errorf("should not contain Buddy (no owner), got: %s", output)
		}
		if !strings.Contains(output, "Fluffy") {
			t.Errorf("expected Fluffy, got: %s", output)
		}
	})

	t.Run("with filter is empty", func(t *testing.T) {
		filter, _ := ParseWhenFlags([]string{"Owner is empty"})
		output := captureStdout(func() {
			err := ColumnValues([]string{testdataPath("animals.csv")}, ValsOpts{
				Column: "Name",
				Mode:   ValsUniq,
				Filter: filter,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if !strings.Contains(output, "Buddy") {
			t.Errorf("expected Buddy (no owner), got: %s", output)
		}
		lines := strings.Split(strings.TrimSpace(output), "\n")
		if len(lines) != 1 {
			t.Errorf("expected 1 result (Buddy), got %d: %s", len(lines), output)
		}
	})

	t.Run("no files error", func(t *testing.T) {
		err := ColumnValues([]string{}, ValsOpts{Column: "A", Mode: ValsUniq, Config: cfg})
		if err == nil {
			t.Error("expected error for no files")
		}
	})

	t.Run("filter with fixed-width error", func(t *testing.T) {
		filter, _ := ParseWhenFlags([]string{"A=1"})
		err := ColumnValues([]string{testdataPath("basic.csv")}, ValsOpts{
			Column:     "Name",
			Mode:       ValsUniq,
			Filter:     filter,
			FixedStart: 0,
			FixedEnd:   5,
			Config:     cfg,
		})
		if err == nil {
			t.Error("expected error for filter with fixed-width")
		}
		if !strings.Contains(err.Error(), "cannot be used together") {
			t.Errorf("expected incompatibility error, got: %v", err)
		}
	})
}

// TestFirstNonEmpty tests the FirstNonEmpty function
func TestFirstNonEmpty(t *testing.T) {
	cfg := testConfig()

	t.Run("finds first value", func(t *testing.T) {
		var count int
		output := captureStdout(func() {
			var err error
			count, err = FirstNonEmpty([]string{testdataPath("empty_cells.csv")}, FirstOpts{
				Column: "Value",
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if count != 1 {
			t.Errorf("expected 1 value printed, got %d", count)
		}
		if !strings.Contains(output, "100") {
			t.Errorf("expected first value 100, got: %s", output)
		}
	})

	t.Run("column with empty first row", func(t *testing.T) {
		var count int
		output := captureStdout(func() {
			var err error
			count, err = FirstNonEmpty([]string{testdataPath("empty_cells.csv")}, FirstOpts{
				Column: "Extra",
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if count != 1 {
			t.Errorf("expected 1 value printed, got %d", count)
		}
		if !strings.Contains(output, "x") {
			t.Errorf("expected first non-empty Extra value 'x', got: %s", output)
		}
	})
}

// TestFilesWith tests the FilesWith function
func TestFilesWith(t *testing.T) {
	cfg := testConfig()

	t.Run("finds matching file", func(t *testing.T) {
		var count int
		output := captureStdout(func() {
			var err error
			count, err = FilesWith([]string{testdataPath("basic.csv")}, FilesWithOpts{
				Column: "Name",
				Value:  "Alice",
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if count != 1 {
			t.Errorf("expected 1 file, got %d", count)
		}
		if !strings.Contains(output, "basic.csv") {
			t.Errorf("expected basic.csv in output, got: %s", output)
		}
	})

	t.Run("no match", func(t *testing.T) {
		var count int
		captureStdout(func() {
			var err error
			count, err = FilesWith([]string{testdataPath("basic.csv")}, FilesWithOpts{
				Column: "Name",
				Value:  "NotExists",
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if count != 0 {
			t.Errorf("expected 0 files, got %d", count)
		}
	})

	t.Run("case insensitive", func(t *testing.T) {
		var count int
		output := captureStdout(func() {
			var err error
			count, err = FilesWith([]string{testdataPath("basic.csv")}, FilesWithOpts{
				Column:          "Name",
				Value:           "ALICE",
				CaseInsensitive: true,
				Config:          cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if count != 1 {
			t.Errorf("expected 1 file with case insensitive match, got %d", count)
		}
		if !strings.Contains(output, "basic.csv") {
			t.Errorf("expected basic.csv in output, got: %s", output)
		}
	})
}

// TestDupKey tests the DupKey function
func TestDupKey(t *testing.T) {
	cfg := testConfig()
	cfg.Quiet = true

	t.Run("finds duplicate keys", func(t *testing.T) {
		var res DupKeyResult
		captureStderr(func() {
			captureStdout(func() {
				var err error
				res, err = DupKey([]string{testdataPath("duplicates.csv")}, DupKeyOpts{
					Key:       "ID",
					ByColumns: []string{"FirstName", "LastName"},
					Quiet:     true,
					Config:    cfg,
				})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			})
		})
		// ID 001 has John/Smith and Johnny/Smith
		// ID 002 has Jane/Doe and Jane/Williams
		if res.ConflictingKeys != 2 {
			t.Errorf("expected 2 conflicting keys, got %d", res.ConflictingKeys)
		}
	})

	t.Run("no duplicates", func(t *testing.T) {
		var res DupKeyResult
		captureStderr(func() {
			captureStdout(func() {
				var err error
				res, err = DupKey([]string{testdataPath("basic.csv")}, DupKeyOpts{
					Key:       "Name",
					ByColumns: []string{"Type"},
					Quiet:     true,
					Config:    cfg,
				})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			})
		})
		if res.ConflictingKeys != 0 {
			t.Errorf("expected 0 conflicting keys, got %d", res.ConflictingKeys)
		}
	})

	t.Run("ignore case", func(t *testing.T) {
		var res DupKeyResult
		captureStderr(func() {
			captureStdout(func() {
				var err error
				res, err = DupKey([]string{testdataPath("duplicates.csv")}, DupKeyOpts{
					Key:        "ID",
					ByColumns:  []string{"FirstName", "LastName"},
					IgnoreCase: true,
					Quiet:      true,
					Config:     cfg,
				})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			})
		})
		// Same as before since case doesn't affect this data
		if res.ConflictingKeys != 2 {
			t.Errorf("expected 2 conflicting keys with ignore case, got %d", res.ConflictingKeys)
		}
	})
}

// TestListColumns tests the ListColumns function
func TestListColumns(t *testing.T) {
	cfg := testConfig()

	t.Run("lists columns", func(t *testing.T) {
		output := captureStdout(func() {
			err := ListColumns([]string{testdataPath("basic.csv")}, ListColsOpts{
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if !strings.Contains(output, "Name") || !strings.Contains(output, "Type") || !strings.Contains(output, "Status") {
			t.Errorf("expected Name, Type, Status in output, got: %s", output)
		}
	})

	t.Run("sorted output", func(t *testing.T) {
		output := captureStdout(func() {
			err := ListColumns([]string{testdataPath("basic.csv")}, ListColsOpts{
				Sorted: true,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		lines := strings.Split(strings.TrimSpace(output), "\n")
		if len(lines) != 3 {
			t.Errorf("expected 3 columns, got %d", len(lines))
		}
		// Sorted: Name, Status, Type
		if lines[0] != "Name" || lines[1] != "Status" || lines[2] != "Type" {
			t.Errorf("expected sorted order Name,Status,Type, got: %v", lines)
		}
	})

	t.Run("oneline output", func(t *testing.T) {
		output := captureStdout(func() {
			err := ListColumns([]string{testdataPath("basic.csv")}, ListColsOpts{
				OneLine:      true,
				OneLineDelim: ",",
				Config:       cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		// Should be on one line
		lines := strings.Split(strings.TrimSpace(output), "\n")
		if len(lines) != 1 {
			t.Errorf("expected 1 line, got %d", len(lines))
		}
		if !strings.Contains(output, ",") {
			t.Errorf("expected comma delimiter, got: %s", output)
		}
	})

	t.Run("multiple files unique columns", func(t *testing.T) {
		output := captureStdout(func() {
			err := ListColumns([]string{
				testdataPath("basic.csv"),
				testdataPath("animals.csv"),
			}, ListColsOpts{
				Sorted: true,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		// Should have columns from both files, no duplicates
		if !strings.Contains(output, "Name") && !strings.Contains(output, "Species") {
			t.Errorf("expected columns from both files, got: %s", output)
		}
		// Count occurrences of "Name" - should be 1
		if strings.Count(output, "Name") != 1 {
			t.Errorf("expected Name to appear once, got: %s", output)
		}
	})

	t.Run("no files error", func(t *testing.T) {
		err := ListColumns([]string{}, ListColsOpts{Config: cfg})
		if err == nil {
			t.Error("expected error for no files")
		}
	})
}

// TestColumnValuesMultipleFiles tests vals across multiple files
func TestColumnValuesMultipleFiles(t *testing.T) {
	cfg := testConfig()

	t.Run("aggregates across files", func(t *testing.T) {
		output := captureStdout(func() {
			err := ColumnValues([]string{
				testdataPath("basic.csv"),
				testdataPath("animals.csv"),
			}, ValsOpts{
				Column: "Name",
				Mode:   ValsUniq,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		// Should have names from both files
		if !strings.Contains(output, "Alice") || !strings.Contains(output, "Fluffy") {
			t.Errorf("expected names from both files, got: %s", output)
		}
	})
}

// TestRewriteDelimiter tests the RewriteDelimiter function
func TestRewriteDelimiter(t *testing.T) {
	cfg := testConfig()

	t.Run("writes to stdout", func(t *testing.T) {
		output := captureStdout(func() {
			err := RewriteDelimiter([]string{testdataPath("basic.csv")}, FmtOpts{
				OutDelim: '\t',
				Config:   cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		// Should have tabs instead of commas
		if !strings.Contains(output, "\t") {
			t.Errorf("expected tabs in output, got: %s", output)
		}
		if strings.Contains(output, ",") {
			t.Errorf("expected no commas in output, got: %s", output)
		}
	})

	t.Run("writes to output file", func(t *testing.T) {
		tmpDir := t.TempDir()
		outPath := filepath.Join(tmpDir, "out.tsv")

		err := RewriteDelimiter([]string{testdataPath("basic.csv")}, FmtOpts{
			OutDelim:   '\t',
			OutputPath: outPath,
			Config:     cfg,
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		content, err := os.ReadFile(outPath)
		if err != nil {
			t.Fatalf("read output file: %v", err)
		}
		if !strings.Contains(string(content), "\t") {
			t.Errorf("expected tabs in output file, got: %s", string(content))
		}
	})

	t.Run("writes to output directory", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfg.Quiet = true

		captureStderr(func() {
			err := RewriteDelimiter([]string{testdataPath("basic.csv")}, FmtOpts{
				OutDelim: '\t',
				OutDir:   tmpDir,
				OutExt:   ".tsv",
				Config:   cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})

		outPath := filepath.Join(tmpDir, "basic.tsv")
		content, err := os.ReadFile(outPath)
		if err != nil {
			t.Fatalf("read output file: %v", err)
		}
		if !strings.Contains(string(content), "\t") {
			t.Errorf("expected tabs in output file, got: %s", string(content))
		}
	})

	t.Run("in-place rewrite", func(t *testing.T) {
		tmpDir := t.TempDir()
		tmpFile := filepath.Join(tmpDir, "test.csv")

		// Copy test file
		content, _ := os.ReadFile(testdataPath("basic.csv"))
		os.WriteFile(tmpFile, content, 0644)

		cfg.Quiet = true
		captureStderr(func() {
			err := RewriteDelimiter([]string{tmpFile}, FmtOpts{
				OutDelim: '\t',
				InPlace:  true,
				Config:   cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})

		newContent, err := os.ReadFile(tmpFile)
		if err != nil {
			t.Fatalf("read file: %v", err)
		}
		if !strings.Contains(string(newContent), "\t") {
			t.Errorf("expected tabs after in-place rewrite, got: %s", string(newContent))
		}
	})

	t.Run("no files error", func(t *testing.T) {
		err := RewriteDelimiter([]string{}, FmtOpts{Config: cfg})
		if err == nil {
			t.Error("expected error for no files")
		}
	})
}

// TestColumnValuesWithNoHeader tests vals with --no-header flag
func TestColumnValuesWithNoHeader(t *testing.T) {
	cfg := testConfig()
	cfg.NoHeader = true

	t.Run("numeric column index", func(t *testing.T) {
		output := captureStdout(func() {
			err := ColumnValues([]string{testdataPath("basic.csv")}, ValsOpts{
				Column: "1",
				Mode:   ValsUniq,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		// Should include "Type" (header) and the values
		if !strings.Contains(output, "Type") || !strings.Contains(output, "cat") {
			t.Errorf("expected Type and cat in output, got: %s", output)
		}
	})

	t.Run("filter with numeric index", func(t *testing.T) {
		filter, _ := ParseWhenFlags([]string{"1=cat"})
		output := captureStdout(func() {
			err := ColumnValues([]string{testdataPath("basic.csv")}, ValsOpts{
				Column: "0",
				Mode:   ValsUniq,
				Filter: filter,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if !strings.Contains(output, "Alice") {
			t.Errorf("expected Alice in filtered output, got: %s", output)
		}
	})
}

// TestColumnValuesFixedWidth tests vals with --fixed-width flag
func TestColumnValuesFixedWidth(t *testing.T) {
	cfg := testConfig()

	t.Run("extracts fixed width field", func(t *testing.T) {
		output := captureStdout(func() {
			err := ColumnValues([]string{testdataPath("fixed_width.txt")}, ValsOpts{
				Column:     "ignored", // not used in fixed-width mode
				Mode:       ValsUniq,
				FixedStart: 4,  // NAME column starts at index 4
				FixedEnd:   14, // NAME column ends before index 14
				Config:     cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if !strings.Contains(output, "Alice") || !strings.Contains(output, "Bob") {
			t.Errorf("expected Alice and Bob in output, got: %s", output)
		}
	})

	t.Run("frequency mode", func(t *testing.T) {
		output := captureStdout(func() {
			err := ColumnValues([]string{testdataPath("fixed_width.txt")}, ValsOpts{
				Column:     "ignored",
				Mode:       ValsFreq,
				FixedStart: 0,
				FixedEnd:   3, // ID column
				Config:     cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		// Each ID appears once
		if !strings.Contains(output, "001") || !strings.Contains(output, "1") {
			t.Errorf("expected ID with count in output, got: %s", output)
		}
	})
}

// createTestZip creates a zip file with the given files
func createTestZip(t *testing.T, path string, files map[string]string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create zip: %v", err)
	}
	defer f.Close()

	w := zip.NewWriter(f)
	for name, content := range files {
		fw, err := w.Create(name)
		if err != nil {
			t.Fatalf("create file in zip: %v", err)
		}
		if _, err := fw.Write([]byte(content)); err != nil {
			t.Fatalf("write file in zip: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close zip: %v", err)
	}
}

// TestCompareZips tests the CompareZips function
func TestCompareZips(t *testing.T) {
	t.Run("identical zips", func(t *testing.T) {
		tmpDir := t.TempDir()
		zip1 := filepath.Join(tmpDir, "a.zip")
		zip2 := filepath.Join(tmpDir, "b.zip")

		files := map[string]string{
			"file1.txt": "hello world",
			"file2.txt": "goodbye world",
		}
		createTestZip(t, zip1, files)
		createTestZip(t, zip2, files)

		var res ZipCmpResult
		captureStderr(func() {
			captureStdout(func() {
				var err error
				res, err = CompareZips(ZipCmpOpts{
					ZipA:  zip1,
					ZipB:  zip2,
					Quiet: true,
				})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			})
		})

		if res.Different != 0 || res.OnlyInA != 0 || res.OnlyInB != 0 {
			t.Errorf("expected no differences, got OnlyInA=%d, OnlyInB=%d, Different=%d",
				res.OnlyInA, res.OnlyInB, res.Different)
		}
		if res.Identical != 2 {
			t.Errorf("expected 2 identical files, got %d", res.Identical)
		}
	})

	t.Run("different content", func(t *testing.T) {
		tmpDir := t.TempDir()
		zip1 := filepath.Join(tmpDir, "a.zip")
		zip2 := filepath.Join(tmpDir, "b.zip")

		createTestZip(t, zip1, map[string]string{
			"file1.txt": "hello world",
		})
		createTestZip(t, zip2, map[string]string{
			"file1.txt": "different content",
		})

		var res ZipCmpResult
		captureStderr(func() {
			captureStdout(func() {
				var err error
				res, err = CompareZips(ZipCmpOpts{
					ZipA:        zip1,
					ZipB:        zip2,
					Quiet:       true,
					SummaryOnly: true,
				})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			})
		})

		if res.Different != 1 {
			t.Errorf("expected 1 different file, got %d", res.Different)
		}
	})

	t.Run("files only in one zip", func(t *testing.T) {
		tmpDir := t.TempDir()
		zip1 := filepath.Join(tmpDir, "a.zip")
		zip2 := filepath.Join(tmpDir, "b.zip")

		createTestZip(t, zip1, map[string]string{
			"common.txt":    "shared",
			"only_in_a.txt": "a only",
		})
		createTestZip(t, zip2, map[string]string{
			"common.txt":    "shared",
			"only_in_b.txt": "b only",
		})

		var res ZipCmpResult
		captureStderr(func() {
			captureStdout(func() {
				var err error
				res, err = CompareZips(ZipCmpOpts{
					ZipA:  zip1,
					ZipB:  zip2,
					Quiet: true,
				})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			})
		})

		if res.OnlyInA != 1 || res.OnlyInB != 1 {
			t.Errorf("expected 1 file only in each, got OnlyInA=%d, OnlyInB=%d",
				res.OnlyInA, res.OnlyInB)
		}
	})

	t.Run("with diff output", func(t *testing.T) {
		tmpDir := t.TempDir()
		zip1 := filepath.Join(tmpDir, "a.zip")
		zip2 := filepath.Join(tmpDir, "b.zip")

		createTestZip(t, zip1, map[string]string{
			"file.txt": "line1\nline2\nline3\n",
		})
		createTestZip(t, zip2, map[string]string{
			"file.txt": "line1\nchanged\nline3\n",
		})

		var stderrOutput string
		stderrOutput = captureStderr(func() {
			captureStdout(func() {
				_, err := CompareZips(ZipCmpOpts{
					ZipA:  zip1,
					ZipB:  zip2,
					Quiet: false,
				})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			})
		})

		// Should show diff preview in stderr
		if !strings.Contains(stderrOutput, "Differences") {
			t.Errorf("expected diff output, got: %s", stderrOutput)
		}
	})

	t.Run("nonexistent file error", func(t *testing.T) {
		captureStderr(func() {
			_, err := CompareZips(ZipCmpOpts{
				ZipA: "nonexistent1.zip",
				ZipB: "nonexistent2.zip",
			})
			if err == nil {
				t.Error("expected error for nonexistent files")
			}
		})
	})
}

// TestCompareColumnsWithFilter tests CompareColumns with --when filtering
func TestCompareColumnsWithFilter(t *testing.T) {
	cfg := testConfig()
	cfg.Quiet = true

	t.Run("filter reduces rows compared", func(t *testing.T) {
		// compare.csv: ColA,ColB,ColC
		// apple,apple,APPLE    → ColA==ColB (match)
		// banana,banana,banana → ColA==ColB (match)
		// cherry,Cherry,cherry → ColA!=ColB (mismatch)
		// date,fig,date        → ColA!=ColB (mismatch)
		//
		// Without filter: 2 mismatches
		// With ColC=cherry: only row 3 selected → 1 mismatch
		filter, err := ParseWhenFlags([]string{"ColC=cherry"})
		if err != nil {
			t.Fatalf("parse filter: %v", err)
		}

		var res CompareResult
		captureStderr(func() {
			res, err = CompareColumns([]string{testdataPath("compare.csv")}, CompareOpts{
				ColA:       "ColA",
				TargetCols: []string{"ColB"},
				Filter:     filter,
				Quiet:      true,
				Config:     cfg,
			})
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Mismatches != 1 {
			t.Errorf("expected 1 mismatch with filter, got %d", res.Mismatches)
		}
	})

	t.Run("filter excludes all mismatches", func(t *testing.T) {
		// ColC=banana selects only row 2 (banana,banana,banana) → 0 mismatches
		filter, err := ParseWhenFlags([]string{"ColC=banana"})
		if err != nil {
			t.Fatalf("parse filter: %v", err)
		}

		var res CompareResult
		captureStderr(func() {
			res, err = CompareColumns([]string{testdataPath("compare.csv")}, CompareOpts{
				ColA:       "ColA",
				TargetCols: []string{"ColB"},
				Filter:     filter,
				Quiet:      true,
				Config:     cfg,
			})
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Mismatches != 0 {
			t.Errorf("expected 0 mismatches with filter, got %d", res.Mismatches)
		}
	})

	t.Run("filter with OR", func(t *testing.T) {
		// ColC=cherry|ColC=date selects rows 3 and 4 → 2 mismatches
		filter, err := ParseWhenFlags([]string{"ColC=cherry|ColC=date"})
		if err != nil {
			t.Fatalf("parse filter: %v", err)
		}

		var res CompareResult
		captureStderr(func() {
			res, err = CompareColumns([]string{testdataPath("compare.csv")}, CompareOpts{
				ColA:       "ColA",
				TargetCols: []string{"ColB"},
				Filter:     filter,
				Quiet:      true,
				Config:     cfg,
			})
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Mismatches != 2 {
			t.Errorf("expected 2 mismatches with OR filter, got %d", res.Mismatches)
		}
	})

	t.Run("filter with not equal", func(t *testing.T) {
		// ColB!=apple excludes row 1, leaving rows 2,3,4 → 2 mismatches
		filter, err := ParseWhenFlags([]string{"ColB!=apple"})
		if err != nil {
			t.Fatalf("parse filter: %v", err)
		}

		var res CompareResult
		captureStderr(func() {
			res, err = CompareColumns([]string{testdataPath("compare.csv")}, CompareOpts{
				ColA:       "ColA",
				TargetCols: []string{"ColB"},
				Filter:     filter,
				Quiet:      true,
				Config:     cfg,
			})
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Mismatches != 2 {
			t.Errorf("expected 2 mismatches with != filter, got %d", res.Mismatches)
		}
	})

	t.Run("empty filter matches all rows", func(t *testing.T) {
		var res CompareResult
		var err error
		captureStderr(func() {
			res, err = CompareColumns([]string{testdataPath("compare.csv")}, CompareOpts{
				ColA:       "ColA",
				TargetCols: []string{"ColB"},
				Quiet:      true,
				Config:     cfg,
			})
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Mismatches != 2 {
			t.Errorf("expected 2 mismatches without filter, got %d", res.Mismatches)
		}
	})
}

// TestCompareColumnsEdgeCases tests edge cases in CompareColumns
func TestCompareColumnsEdgeCases(t *testing.T) {
	cfg := testConfig()
	cfg.Quiet = true

	t.Run("with allow-empty", func(t *testing.T) {
		var res CompareResult
		captureStderr(func() {
			var err error
			res, err = CompareColumns([]string{testdataPath("empty_cells.csv")}, CompareOpts{
				ColA:       "Value",
				TargetCols: []string{"Extra"},
				AllowEmpty: true,
				Quiet:      true,
				Config:     cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		// With allow-empty, empty cells are compared (and won't match non-empty)
		if res.RowsSeen == 0 {
			t.Error("expected rows to be processed with allow-empty")
		}
	})

	t.Run("with no-header", func(t *testing.T) {
		cfg := testConfig()
		cfg.NoHeader = true
		cfg.Quiet = true

		var res CompareResult
		captureStderr(func() {
			var err error
			res, err = CompareColumns([]string{testdataPath("compare.csv")}, CompareOpts{
				ColA:       "0",
				TargetCols: []string{"1"},
				Quiet:      true,
				Config:     cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if res.FilesScanned != 1 {
			t.Errorf("expected 1 file scanned, got %d", res.FilesScanned)
		}
	})
}

func TestCompareColumnsOutputFormats(t *testing.T) {
	cfg := testConfig()

	t.Run("lines format", func(t *testing.T) {
		var output string
		captureStderr(func() {
			output = captureStdout(func() {
				_, err := CompareColumns([]string{testdataPath("compare.csv")}, CompareOpts{
					ColA:       "ColA",
					TargetCols: []string{"ColB"},
					Format:     OutputLines,
					Config:     cfg,
				})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			})
		})
		if !strings.Contains(output, "compare.csv line") {
			t.Errorf("expected file and line in output, got: %s", output)
		}
		if !strings.Contains(output, "ColA:") || !strings.Contains(output, "ColB:") {
			t.Errorf("expected ColA: and ColB: labels in output, got: %s", output)
		}
	})

	t.Run("table format", func(t *testing.T) {
		var output string
		captureStderr(func() {
			output = captureStdout(func() {
				_, err := CompareColumns([]string{testdataPath("compare.csv")}, CompareOpts{
					ColA:       "ColA",
					TargetCols: []string{"ColB"},
					Format:     OutputTable,
					Config:     cfg,
				})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			})
		})
		if !strings.Contains(output, "ColA") || !strings.Contains(output, "ColB") {
			t.Errorf("expected column headers in output, got: %s", output)
		}
		if !strings.Contains(output, "|") {
			t.Errorf("expected pipe separator in table output, got: %s", output)
		}
		if !strings.Contains(output, "-+-") {
			t.Errorf("expected header separator in table output, got: %s", output)
		}
		if strings.Contains(output, "compare.csv") {
			t.Errorf("table format should not contain filename, got: %s", output)
		}
	})

	t.Run("csv format", func(t *testing.T) {
		tmpDir := t.TempDir()
		csvPath := filepath.Join(tmpDir, "mismatches.csv")

		captureStderr(func() {
			_, err := CompareColumns([]string{testdataPath("compare.csv")}, CompareOpts{
				ColA:       "ColA",
				TargetCols: []string{"ColB"},
				Format:     OutputCSV,
				CSVPath:    csvPath,
				Config:     cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})

		content, err := os.ReadFile(csvPath)
		if err != nil {
			t.Fatalf("failed to read csv: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(string(content)), "\n")
		if len(lines) != 3 {
			t.Errorf("expected 3 lines in CSV (header + 2 mismatches), got %d: %s", len(lines), string(content))
		}
		if lines[0] != "ColA,ColB,file,line" {
			t.Errorf("unexpected CSV header: %s", lines[0])
		}
		if !strings.Contains(lines[1], "compare.csv") || !strings.Contains(lines[1], "4") {
			t.Errorf("expected file and line in CSV row, got: %s", lines[1])
		}
	})

	t.Run("csv format no mismatches", func(t *testing.T) {
		tmpDir := t.TempDir()
		csvPath := filepath.Join(tmpDir, "empty.csv")

		captureStderr(func() {
			_, err := CompareColumns([]string{testdataPath("compare.csv")}, CompareOpts{
				ColA:       "ColA",
				TargetCols: []string{"ColA"},
				Format:     OutputCSV,
				CSVPath:    csvPath,
				Config:     cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})

		if _, err := os.Stat(csvPath); !os.IsNotExist(err) {
			t.Errorf("CSV file should not be created when no mismatches")
		}
	})

	t.Run("table format no mismatches", func(t *testing.T) {
		var output string
		captureStderr(func() {
			output = captureStdout(func() {
				_, err := CompareColumns([]string{testdataPath("compare.csv")}, CompareOpts{
					ColA:       "ColA",
					TargetCols: []string{"ColA"},
					Format:     OutputTable,
					Config:     cfg,
				})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			})
		})
		if output != "" {
			t.Errorf("expected empty output when no mismatches, got: %s", output)
		}
	})

	t.Run("table format multiple targets", func(t *testing.T) {
		var output string
		captureStderr(func() {
			output = captureStdout(func() {
				_, err := CompareColumns([]string{testdataPath("compare.csv")}, CompareOpts{
					ColA:       "ColC",
					TargetCols: []string{"ColA", "ColB"},
					Format:     OutputTable,
					Config:     cfg,
				})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			})
		})
		// Should show all three column headers
		if !strings.Contains(output, "ColC") || !strings.Contains(output, "ColA") || !strings.Contains(output, "ColB") {
			t.Errorf("expected all column headers in output, got: %s", output)
		}
	})
}

// TestDupKeyEdgeCases tests edge cases in DupKey
func TestDupKeyEdgeCases(t *testing.T) {
	cfg := testConfig()
	cfg.Quiet = true

	t.Run("with require-all", func(t *testing.T) {
		var res DupKeyResult
		captureStderr(func() {
			captureStdout(func() {
				var err error
				res, err = DupKey([]string{testdataPath("empty_cells.csv")}, DupKeyOpts{
					Key:        "Name",
					ByColumns:  []string{"Value", "Extra"},
					RequireAll: true,
					Quiet:      true,
					Config:     cfg,
				})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			})
		})
		// All rows are seen, but rows with empty BY fields are skipped for duplicate detection
		// With require-all, only Alice and Dave have both Value and Extra non-empty
		// So there should be no conflicting keys (each name is unique)
		if res.ConflictingKeys != 0 {
			t.Errorf("expected 0 conflicting keys with require-all filtering, got %d", res.ConflictingKeys)
		}
	})

	t.Run("no files error", func(t *testing.T) {
		_, err := DupKey([]string{}, DupKeyOpts{
			Key:       "A",
			ByColumns: []string{"B"},
			Config:    cfg,
		})
		if err == nil {
			t.Error("expected error for no files")
		}
	})
}
