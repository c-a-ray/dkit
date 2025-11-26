package ops

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/c-a-ray/dkit/internal/core"
)

// ZipCmpOpts configures how two ZIP archives are compared
type ZipCmpOpts struct {
	ZipA          string
	ZipB          string
	Quiet         bool
	SummaryOnly   bool
	IgnoreMissing bool
	Config        *core.Config
}

// ZipCmpResult summarizes the outcome of a ZIP comparison
type ZipCmpResult struct {
	Identical int
	Different int
	OnlyInA   int
	OnlyInB   int
}

// CompareZips compares two ZIP archives and reports differences
func CompareZips(opts ZipCmpOpts) (ZipCmpResult, error) {
	result := ZipCmpResult{}

	readerA, err := zip.OpenReader(opts.ZipA)
	if err != nil {
		return result, fmt.Errorf("failed to open %s: %w", opts.ZipA, err)
	}
	defer readerA.Close()

	readerB, err := zip.OpenReader(opts.ZipB)
	if err != nil {
		return result, fmt.Errorf("failed to open %s: %w", opts.ZipB, err)
	}
	defer readerB.Close()

	filesA := make(map[string]*zip.File)
	filesB := make(map[string]*zip.File)

	for _, f := range readerA.File {
		if !f.FileInfo().IsDir() {
			filesA[f.Name] = f
		}
	}

	for _, f := range readerB.File {
		if !f.FileInfo().IsDir() {
			filesB[f.Name] = f
		}
	}

	allFiles := make(map[string]bool)
	for name := range filesA {
		allFiles[name] = true
	}
	for name := range filesB {
		allFiles[name] = true
	}

	var sortedFiles []string
	for name := range allFiles {
		sortedFiles = append(sortedFiles, name)
	}
	sort.Strings(sortedFiles)

	if !opts.Quiet {
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "═══════════════════════════════════════════════════════════\n")
		fmt.Fprintf(os.Stderr, "Comparing ZIP archives\n")
		fmt.Fprintf(os.Stderr, "═══════════════════════════════════════════════════════════\n")
		fmt.Fprintf(os.Stderr, "\nArchive A: %s\n", filepath.Base(opts.ZipA))
		fmt.Fprintf(os.Stderr, "Archive B: %s\n\n", filepath.Base(opts.ZipB))
	}

	var commonFiles []string
	for _, name := range sortedFiles {
		inA := filesA[name] != nil
		inB := filesB[name] != nil

		if inA && inB {
			commonFiles = append(commonFiles, name)
		} else if inA && !inB {
			result.OnlyInA++
			if !opts.Quiet {
				fmt.Printf("→ %s (only in A)\n", name)
			}
		} else if !inA && inB {
			result.OnlyInB++
			if !opts.Quiet {
				fmt.Printf("→ %s (only in B)\n", name)
			}
		}
	}

	if len(commonFiles) > 0 && !opts.Quiet {
		fmt.Fprintf(os.Stderr, "\nComparing common files:\n\n")
	}

	for _, name := range commonFiles {
		identical, err := compareZipFiles(filesA[name], filesB[name], name, opts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] error comparing %s: %v\n", name, err)
			continue
		}

		if identical {
			result.Identical++
			if !opts.Quiet {
				fmt.Printf("✅ %s (identical)\n", name)
			}
		} else {
			result.Different++
			if !opts.Quiet {
				fmt.Printf("⚠ %s (different)\n", name)
			}
		}
	}

	// Print summary
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "═══════════════════════════════════════════════════════════\n")
	fmt.Fprintf(os.Stderr, "Summary\n")
	fmt.Fprintf(os.Stderr, "═══════════════════════════════════════════════════════════\n")
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "Identical files:  %d\n", result.Identical)
	fmt.Fprintf(os.Stderr, "Different files:  %d\n", result.Different)
	fmt.Fprintf(os.Stderr, "Only in A:        %d\n", result.OnlyInA)
	fmt.Fprintf(os.Stderr, "Only in B:        %d\n", result.OnlyInB)
	fmt.Fprintf(os.Stderr, "\n")

	return result, nil
}

func compareZipFiles(fileA, fileB *zip.File, name string, opts ZipCmpOpts) (bool, error) {
	rcA, err := fileA.Open()
	if err != nil {
		return false, fmt.Errorf("failed to open %s from archive A: %w", name, err)
	}
	defer rcA.Close()

	var bufA bytes.Buffer
	if _, err := io.Copy(&bufA, rcA); err != nil {
		return false, fmt.Errorf("failed to read %s from archive A: %w", name, err)
	}

	rcB, err := fileB.Open()
	if err != nil {
		return false, fmt.Errorf("failed to open %s from archive B: %w", name, err)
	}
	defer rcB.Close()

	var bufB bytes.Buffer
	if _, err := io.Copy(&bufB, rcB); err != nil {
		return false, fmt.Errorf("failed to read %s from archive B: %w", name, err)
	}

	identical := bytes.Equal(bufA.Bytes(), bufB.Bytes())

	if !identical && !opts.SummaryOnly && !opts.Quiet {
		showDiffPreview(name, bufA.Bytes(), bufB.Bytes())
	}

	return identical, nil
}

func showDiffPreview(name string, contentA, contentB []byte) {
	fmt.Fprintf(os.Stderr, "\n  Differences in %s:\n", name)

	if isLikelyText(contentA) && isLikelyText(contentB) {
		linesA := strings.Split(string(contentA), "\n")
		linesB := strings.Split(string(contentB), "\n")

		maxLines := 10
		lineCount := 0

		for i := 0; i < len(linesA) || i < len(linesB); i++ {
			if lineCount >= maxLines {
				fmt.Fprintf(os.Stderr, "  ... (diff truncated)\n")
				break
			}

			var lineA, lineB string
			if i < len(linesA) {
				lineA = linesA[i]
			}
			if i < len(linesB) {
				lineB = linesB[i]
			}

			if lineA != lineB {
				fmt.Fprintf(os.Stderr, "  Line %d:\n", i+1)
				if lineA != "" {
					fmt.Fprintf(os.Stderr, "    A: %s\n", truncateLine(lineA, 80))
				}
				if lineB != "" {
					fmt.Fprintf(os.Stderr, "    B: %s\n", truncateLine(lineB, 80))
				}
				lineCount++
			}
		}
	} else {
		// For binary files just show file size difference
		fmt.Fprintf(os.Stderr, "  File sizes: A=%d bytes, B=%d bytes\n", len(contentA), len(contentB))
	}

	fmt.Fprintf(os.Stderr, "\n")
}

func isLikelyText(content []byte) bool {
	if len(content) == 0 {
		return true
	}

	sample := content
	if len(content) > 512 {
		sample = content[:512]
	}

	for _, b := range sample {
		if b == 0 {
			return false
		}
	}

	return true
}

func truncateLine(line string, maxLen int) string {
	if len(line) <= maxLen {
		return line
	}
	return line[:maxLen-3] + "..."
}
