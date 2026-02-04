package core

import (
	"path/filepath"
	"testing"
)

func TestExpandFiles(t *testing.T) {
	t.Run("single file", func(t *testing.T) {
		files, err := ExpandFiles([]string{filepath.Join("testdata", "simple.csv")})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(files) != 1 {
			t.Errorf("expected 1 file, got %d", len(files))
		}
	})

	t.Run("glob pattern", func(t *testing.T) {
		files, err := ExpandFiles([]string{filepath.Join("testdata", "*.csv")})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(files) < 1 {
			t.Errorf("expected at least 1 file, got %d", len(files))
		}
	})

	t.Run("multiple patterns", func(t *testing.T) {
		files, err := ExpandFiles([]string{
			filepath.Join("testdata", "*.csv"),
			filepath.Join("testdata", "*.tsv"),
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(files) < 2 {
			t.Errorf("expected at least 2 files, got %d", len(files))
		}
	})

	t.Run("deduplicates", func(t *testing.T) {
		files, err := ExpandFiles([]string{
			filepath.Join("testdata", "simple.csv"),
			filepath.Join("testdata", "simple.csv"),
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(files) != 1 {
			t.Errorf("expected 1 file after dedup, got %d", len(files))
		}
	})

	t.Run("empty input", func(t *testing.T) {
		_, err := ExpandFiles([]string{})
		if err == nil {
			t.Error("expected error for empty input")
		}
	})

	t.Run("nil input", func(t *testing.T) {
		_, err := ExpandFiles(nil)
		if err == nil {
			t.Error("expected error for nil input")
		}
	})

	t.Run("no matches", func(t *testing.T) {
		files, err := ExpandFiles([]string{filepath.Join("testdata", "nonexistent*.xyz")})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(files) != 0 {
			t.Errorf("expected 0 files for no matches, got %d", len(files))
		}
	})
}
