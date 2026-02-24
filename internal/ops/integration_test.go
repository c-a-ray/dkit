package ops

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/c-a-ray/dkit/internal/core"
)

// TestSkipStartEnd tests --skipStart and --skipEnd with pipe-delimited files
// that have junk header/footer rows.
func TestSkipStartEnd(t *testing.T) {
	cfg := testConfig()
	cfg.Delim = '|'
	cfg.SkipStart = 1
	cfg.SkipEnd = 1

	files := []string{testdataPath("messy.txt")}

	t.Run("vals uniq with skip", func(t *testing.T) {
		output := captureStdout(func() {
			err := ColumnValues(files, ValsOpts{
				Column: "FirstName",
				Mode:   ValsUniq,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		for _, name := range []string{"Alice", "Bob", "Carol", "Dave", "Eve"} {
			if !strings.Contains(output, name) {
				t.Errorf("expected %q in output, got:\n%s", name, output)
			}
		}
	})

	t.Run("vals freq with skip", func(t *testing.T) {
		output := captureStdout(func() {
			err := ColumnValues(files, ValsOpts{
				Column: "Status",
				Mode:   ValsFreq,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if !strings.Contains(output, "active") || !strings.Contains(output, "inactive") {
			t.Errorf("expected active/inactive in output, got:\n%s", output)
		}
	})

	t.Run("list columns with skip", func(t *testing.T) {
		output := captureStdout(func() {
			err := ListColumns(files, ListColsOpts{Config: cfg})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		for _, col := range []string{"ID", "FirstName", "LastName", "City", "Status"} {
			if !strings.Contains(output, col) {
				t.Errorf("expected column %q in output, got:\n%s", col, output)
			}
		}
	})

	t.Run("first non-empty with skip", func(t *testing.T) {
		output := captureStdout(func() {
			_, err := FirstNonEmpty(files, FirstOpts{
				Column: "City",
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if !strings.Contains(output, "New York") {
			t.Errorf("expected 'New York' in output, got:\n%s", output)
		}
	})

	t.Run("files with using skip", func(t *testing.T) {
		output := captureStdout(func() {
			n, err := FilesWith(files, FilesWithOpts{
				Column: "City",
				Value:  "Chicago",
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if n != 1 {
				t.Errorf("expected 1 match, got %d", n)
			}
		})
		if !strings.Contains(output, "messy.txt") {
			t.Errorf("expected file path in output, got:\n%s", output)
		}
	})

	t.Run("compare columns with skip", func(t *testing.T) {
		cfg2 := testConfig()
		cfg2.Delim = '|'
		cfg2.SkipStart = 1
		cfg2.SkipEnd = 1
		cfg2.Quiet = true

		res, err := CompareColumns(files, CompareOpts{
			ColA:       "FirstName",
			TargetCols: []string{"LastName"},
			Config:     cfg2,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// All names differ from last names
		if res.Mismatches != 5 {
			t.Errorf("expected 5 mismatches, got %d", res.Mismatches)
		}
	})

	t.Run("multi-file skip with vals", func(t *testing.T) {
		multiFiles := []string{
			testdataPath("messy.txt"),
			testdataPath("messy2.txt"),
		}
		output := captureStdout(func() {
			err := ColumnValues(multiFiles, ValsOpts{
				Column: "FirstName",
				Mode:   ValsUniq,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		// Should have names from both files
		for _, name := range []string{"Alice", "Bob", "Frank", "Grace", "Hank"} {
			if !strings.Contains(output, name) {
				t.Errorf("expected %q in output, got:\n%s", name, output)
			}
		}
	})

	t.Run("dupkey with skip", func(t *testing.T) {
		multiFiles := []string{
			testdataPath("messy.txt"),
			testdataPath("messy2.txt"),
		}
		cfg2 := testConfig()
		cfg2.Delim = '|'
		cfg2.SkipStart = 1
		cfg2.SkipEnd = 1
		cfg2.Quiet = true

		res, err := DupKey(multiFiles, DupKeyOpts{
			Key:       "Status",
			ByColumns: []string{"FirstName"},
			Config:    cfg2,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// "active" maps to Alice,Bob,Dave,Frank,Hank (5 distinct)
		// "inactive" maps to Carol,Eve,Grace (3 distinct)
		// Both have multiple distinct BY-tuples, so 2 conflicting keys
		if res.ConflictingKeys != 2 {
			t.Errorf("expected 2 conflicting keys, got %d", res.ConflictingKeys)
		}
	})
}

// TestFieldsPerRecordVariable tests --fieldsPerRecord variable with ragged CSV
func TestFieldsPerRecordVariable(t *testing.T) {
	cfg := testConfig()
	cfg.FieldsPerRecord = -1 // variable

	files := []string{testdataPath("variable_cols.csv")}

	t.Run("reads all rows despite variable columns", func(t *testing.T) {
		output := captureStdout(func() {
			err := ColumnValues(files, ValsOpts{
				Column: "Name",
				Mode:   ValsUniq,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		for _, name := range []string{"Alice", "Bob", "Carol", "Dave"} {
			if !strings.Contains(output, name) {
				t.Errorf("expected %q in output, got:\n%s", name, output)
			}
		}
	})

	t.Run("fails without variable mode", func(t *testing.T) {
		cfg2 := testConfig()
		// FieldsPerRecord = 0 (default: enforce first-row count)
		stderr := captureStderr(func() {
			_ = ColumnValues(files, ValsOpts{
				Column: "Name",
				Mode:   ValsUniq,
				Config: cfg2,
			})
		})
		if !strings.Contains(stderr, "wrong number of fields") {
			t.Errorf("expected field count error, got stderr:\n%s", stderr)
		}
	})

	t.Run("freq mode with variable columns", func(t *testing.T) {
		output := captureStdout(func() {
			err := ColumnValues(files, ValsOpts{
				Column: "City",
				Mode:   ValsFreq,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if !strings.Contains(output, "NYC") || !strings.Contains(output, "LA") {
			t.Errorf("expected cities in output, got:\n%s", output)
		}
	})
}

// TestFixedColumnsConfig tests the fixedColumns feature via Config
func TestFixedColumnsConfig(t *testing.T) {
	cfg := testConfig()
	cfg.FixedColumns = []core.FixedColumnDef{
		{Name: "FirstName", Start: 0, End: 10},
		{Name: "LastName", Start: 10, End: 20},
		{Name: "City", Start: 20, End: 30},
		{Name: "Status", Start: 30, End: 40},
	}

	files := []string{testdataPath("fixed_people.txt")}

	t.Run("vals uniq on fixed-width column", func(t *testing.T) {
		output := captureStdout(func() {
			err := ColumnValues(files, ValsOpts{
				Column: "FirstName",
				Mode:   ValsUniq,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		for _, name := range []string{"John", "Jane", "Bob", "Alice"} {
			if !strings.Contains(output, name) {
				t.Errorf("expected %q in output, got:\n%s", name, output)
			}
		}
	})

	t.Run("vals freq on fixed-width column", func(t *testing.T) {
		output := captureStdout(func() {
			err := ColumnValues(files, ValsOpts{
				Column: "Status",
				Mode:   ValsFreq,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if !strings.Contains(output, "active") || !strings.Contains(output, "inactive") {
			t.Errorf("expected active/inactive in output, got:\n%s", output)
		}
		// "active" and "inactive" should each appear 2 times
		if !strings.Contains(output, "2") {
			t.Errorf("expected count of 2 in output, got:\n%s", output)
		}
	})

	t.Run("list columns shows fixed-width names", func(t *testing.T) {
		output := captureStdout(func() {
			err := ListColumns(files, ListColsOpts{Config: cfg})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		for _, col := range []string{"FirstName", "LastName", "City", "Status"} {
			if !strings.Contains(output, col) {
				t.Errorf("expected column %q in output, got:\n%s", col, output)
			}
		}
	})

	t.Run("first non-empty on fixed-width", func(t *testing.T) {
		output := captureStdout(func() {
			_, err := FirstNonEmpty(files, FirstOpts{
				Column: "LastName",
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		if !strings.Contains(output, "Smith") {
			t.Errorf("expected 'Smith' in output, got:\n%s", output)
		}
	})

	t.Run("files with on fixed-width", func(t *testing.T) {
		output := captureStdout(func() {
			n, err := FilesWith(files, FilesWithOpts{
				Column: "City",
				Value:  "Chicago",
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if n != 1 {
				t.Errorf("expected 1 match, got %d", n)
			}
		})
		if !strings.Contains(output, "fixed_people.txt") {
			t.Errorf("expected file path in output, got:\n%s", output)
		}
	})

	t.Run("compare columns on fixed-width", func(t *testing.T) {
		cfg2 := testConfig()
		cfg2.Quiet = true
		cfg2.FixedColumns = cfg.FixedColumns

		res, err := CompareColumns(files, CompareOpts{
			ColA:       "FirstName",
			TargetCols: []string{"LastName"},
			Config:     cfg2,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Mismatches != 4 {
			t.Errorf("expected 4 mismatches, got %d", res.Mismatches)
		}
		if res.RowsSeen != 4 {
			t.Errorf("expected 4 rows, got %d", res.RowsSeen)
		}
	})

	t.Run("only defines subset of columns", func(t *testing.T) {
		// Only define FirstName and Status — skip LastName and City
		cfgSubset := testConfig()
		cfgSubset.FixedColumns = []core.FixedColumnDef{
			{Name: "FirstName", Start: 0, End: 10},
			{Name: "Status", Start: 30, End: 40},
		}

		output := captureStdout(func() {
			err := ColumnValues(files, ValsOpts{
				Column: "FirstName",
				Mode:   ValsUniq,
				Config: cfgSubset,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		for _, name := range []string{"John", "Jane", "Bob", "Alice"} {
			if !strings.Contains(output, name) {
				t.Errorf("expected %q in output, got:\n%s", name, output)
			}
		}
	})
}

// TestCombinedFeatures tests skip + variable + fixed-width together
func TestCombinedFeatures(t *testing.T) {
	t.Run("skip + pipe delimiter + variable fields", func(t *testing.T) {
		cfg := testConfig()
		cfg.Delim = '|'
		cfg.SkipStart = 1
		cfg.SkipEnd = 1
		cfg.FieldsPerRecord = -1

		files := []string{testdataPath("messy.txt")}

		output := captureStdout(func() {
			err := ColumnValues(files, ValsOpts{
				Column: "City",
				Mode:   ValsUniq,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		for _, city := range []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix"} {
			if !strings.Contains(output, city) {
				t.Errorf("expected %q in output, got:\n%s", city, output)
			}
		}
	})

	t.Run("skip + fixed-width columns", func(t *testing.T) {
		cfg := testConfig()
		cfg.SkipStart = 1 // skip the "ID  NAME      VALUE" header line
		cfg.FixedColumns = []core.FixedColumnDef{
			{Name: "ID", Start: 0, End: 3},
			{Name: "NAME", Start: 4, End: 14},
		}

		files := []string{testdataPath("fixed_width.txt")}

		output := captureStdout(func() {
			err := ColumnValues(files, ValsOpts{
				Column: "NAME",
				Mode:   ValsUniq,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		for _, name := range []string{"Alice", "Bob", "Carol"} {
			if !strings.Contains(output, name) {
				t.Errorf("expected %q in output, got:\n%s", name, output)
			}
		}
		// The literal header line "NAME" should NOT appear as a value
		// (it was skipped by skipStart)
		lines := strings.Split(strings.TrimSpace(output), "\n")
		if len(lines) != 3 {
			t.Errorf("expected exactly 3 values, got %d:\n%s", len(lines), output)
		}
	})

	t.Run("filter with skip and pipe delimiter", func(t *testing.T) {
		cfg := testConfig()
		cfg.Delim = '|'
		cfg.SkipStart = 1
		cfg.SkipEnd = 1

		files := []string{testdataPath("messy.txt")}

		filter, err := ParseWhenFlags([]string{"Status=active"})
		if err != nil {
			t.Fatalf("parse filter: %v", err)
		}

		output := captureStdout(func() {
			err := ColumnValues(files, ValsOpts{
				Column: "FirstName",
				Mode:   ValsUniq,
				Filter: filter,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		for _, name := range []string{"Alice", "Bob", "Dave"} {
			if !strings.Contains(output, name) {
				t.Errorf("expected %q in output, got:\n%s", name, output)
			}
		}
		if strings.Contains(output, "Carol") || strings.Contains(output, "Eve") {
			t.Errorf("inactive names should be filtered out, got:\n%s", output)
		}
	})
}

// TestConfigFileIntegration tests the config file loading pipeline
func TestConfigFileIntegration(t *testing.T) {
	t.Run("dotfile write and load round-trip", func(t *testing.T) {
		dir := t.TempDir()
		cfgPath := "/some/path/dkit.yaml"

		err := core.WriteDotfile(dir, cfgPath)
		if err != nil {
			t.Fatalf("write dotfile: %v", err)
		}

		got, err := core.LoadDotfile(dir)
		if err != nil {
			t.Fatalf("load dotfile: %v", err)
		}
		if got != cfgPath {
			t.Errorf("got %q, want %q", got, cfgPath)
		}
	})

	t.Run("dotfile with relative path", func(t *testing.T) {
		dir := t.TempDir()

		err := core.WriteDotfile(dir, "./dkit.yaml")
		if err != nil {
			t.Fatalf("write dotfile: %v", err)
		}

		got, err := core.LoadDotfile(dir)
		if err != nil {
			t.Fatalf("load dotfile: %v", err)
		}
		// Should be resolved to absolute
		if !strings.HasSuffix(got, "dkit.yaml") {
			t.Errorf("expected path ending in dkit.yaml, got %q", got)
		}
		if strings.HasPrefix(got, "./") {
			t.Errorf("expected resolved path, got relative %q", got)
		}
	})

	t.Run("dotfile not found returns empty", func(t *testing.T) {
		dir := t.TempDir()

		got, err := core.LoadDotfile(dir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "" {
			t.Errorf("expected empty string, got %q", got)
		}
	})

	t.Run("remove dotfile", func(t *testing.T) {
		dir := t.TempDir()

		err := core.WriteDotfile(dir, "/some/config.yaml")
		if err != nil {
			t.Fatalf("write: %v", err)
		}

		err = core.RemoveDotfile(dir)
		if err != nil {
			t.Fatalf("remove: %v", err)
		}

		got, err := core.LoadDotfile(dir)
		if err != nil {
			t.Fatalf("load after remove: %v", err)
		}
		if got != "" {
			t.Errorf("expected empty after remove, got %q", got)
		}
	})

	t.Run("remove nonexistent dotfile is no-op", func(t *testing.T) {
		dir := t.TempDir()
		err := core.RemoveDotfile(dir)
		if err != nil {
			t.Errorf("expected no error, got: %v", err)
		}
	})

	t.Run("load and apply full config", func(t *testing.T) {
		dir := t.TempDir()

		// Write a real YAML config
		yamlContent := `
files:
  - "*.txt"
delimiter: pipe
lazyQuotes: true
skipStart: 2
skipEnd: 1
fieldsPerRecord: variable
encoding: latin1
noHeader: true
quiet: true
`
		cfgFile := filepath.Join(dir, "dkit.yaml")
		if err := os.WriteFile(cfgFile, []byte(yamlContent), 0o644); err != nil {
			t.Fatalf("write yaml: %v", err)
		}

		fc, err := core.LoadFileConfig(cfgFile)
		if err != nil {
			t.Fatalf("load config: %v", err)
		}

		cfg := core.NewConfig()
		if err := fc.ApplyTo(cfg); err != nil {
			t.Fatalf("apply: %v", err)
		}

		if cfg.Delim != '|' {
			t.Errorf("Delim = %q, want '|'", cfg.Delim)
		}
		if !cfg.LazyQuotes {
			t.Error("LazyQuotes should be true")
		}
		if cfg.SkipStart != 2 {
			t.Errorf("SkipStart = %d, want 2", cfg.SkipStart)
		}
		if cfg.SkipEnd != 1 {
			t.Errorf("SkipEnd = %d, want 1", cfg.SkipEnd)
		}
		if cfg.FieldsPerRecord != -1 {
			t.Errorf("FieldsPerRecord = %d, want -1", cfg.FieldsPerRecord)
		}
		if cfg.Encoding != "latin1" {
			t.Errorf("Encoding = %q, want latin1", cfg.Encoding)
		}
		if !cfg.NoHeader {
			t.Error("NoHeader should be true")
		}
		if !cfg.Quiet {
			t.Error("Quiet should be true")
		}
		if len(cfg.Files) != 1 || cfg.Files[0] != "*.txt" {
			t.Errorf("Files = %v, want [*.txt]", cfg.Files)
		}
	})

	t.Run("config with fixedColumns", func(t *testing.T) {
		dir := t.TempDir()

		yamlContent := `
fixedColumns:
  - name: FirstName
    start: 0
    end: 20
  - name: LastName
    start: 20
    end: 40
  - name: SSN
    start: 111
    end: 120
`
		cfgFile := filepath.Join(dir, "dkit.yaml")
		if err := os.WriteFile(cfgFile, []byte(yamlContent), 0o644); err != nil {
			t.Fatalf("write yaml: %v", err)
		}

		fc, err := core.LoadFileConfig(cfgFile)
		if err != nil {
			t.Fatalf("load config: %v", err)
		}

		cfg := core.NewConfig()
		if err := fc.ApplyTo(cfg); err != nil {
			t.Fatalf("apply: %v", err)
		}

		if len(cfg.FixedColumns) != 3 {
			t.Fatalf("expected 3 fixed columns, got %d", len(cfg.FixedColumns))
		}
		if cfg.FixedColumns[0].Name != "FirstName" || cfg.FixedColumns[0].Start != 0 || cfg.FixedColumns[0].End != 20 {
			t.Errorf("unexpected first column: %+v", cfg.FixedColumns[0])
		}
		if cfg.FixedColumns[2].Name != "SSN" || cfg.FixedColumns[2].Start != 111 || cfg.FixedColumns[2].End != 120 {
			t.Errorf("unexpected third column: %+v", cfg.FixedColumns[2])
		}
	})

	t.Run("invalid YAML returns error", func(t *testing.T) {
		dir := t.TempDir()
		cfgFile := filepath.Join(dir, "bad.yaml")
		if err := os.WriteFile(cfgFile, []byte("{{invalid yaml"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}

		_, err := core.LoadFileConfig(cfgFile)
		if err == nil {
			t.Error("expected error for invalid YAML")
		}
	})

	t.Run("invalid delimiter in config returns error", func(t *testing.T) {
		dir := t.TempDir()
		cfgFile := filepath.Join(dir, "dkit.yaml")
		if err := os.WriteFile(cfgFile, []byte("delimiter: invalid-multi-char\n"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}

		fc, err := core.LoadFileConfig(cfgFile)
		if err != nil {
			t.Fatalf("load: %v", err)
		}

		cfg := core.NewConfig()
		err = fc.ApplyTo(cfg)
		if err == nil {
			t.Error("expected error for invalid delimiter")
		}
	})

	t.Run("end-to-end: config file drives operation", func(t *testing.T) {
		dir := t.TempDir()

		// Copy messy.txt to temp dir
		data, err := os.ReadFile(testdataPath("messy.txt"))
		if err != nil {
			t.Fatalf("read source: %v", err)
		}
		dataFile := filepath.Join(dir, "data.txt")
		if err := os.WriteFile(dataFile, data, 0o644); err != nil {
			t.Fatalf("write data: %v", err)
		}

		// Write config
		yamlContent := "files:\n  - " + filepath.Join(dir, "*.txt") + "\ndelimiter: pipe\nskipStart: 1\nskipEnd: 1\n"
		cfgFile := filepath.Join(dir, "dkit.yaml")
		if err := os.WriteFile(cfgFile, []byte(yamlContent), 0o644); err != nil {
			t.Fatalf("write yaml: %v", err)
		}

		// Load config and apply
		fc, err := core.LoadFileConfig(cfgFile)
		if err != nil {
			t.Fatalf("load config: %v", err)
		}

		cfg := core.NewConfig()
		if err := fc.ApplyTo(cfg); err != nil {
			t.Fatalf("apply: %v", err)
		}

		// Expand files from config
		files, err := core.ExpandFiles(cfg.Files)
		if err != nil {
			t.Fatalf("expand: %v", err)
		}
		if len(files) != 1 {
			t.Fatalf("expected 1 file, got %d", len(files))
		}

		// Run operation using config-derived settings
		output := captureStdout(func() {
			err := ColumnValues(files, ValsOpts{
				Column: "FirstName",
				Mode:   ValsUniq,
				Config: cfg,
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
		for _, name := range []string{"Alice", "Bob", "Carol", "Dave", "Eve"} {
			if !strings.Contains(output, name) {
				t.Errorf("expected %q in output, got:\n%s", name, output)
			}
		}
	})
}
