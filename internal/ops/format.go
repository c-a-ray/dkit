package ops

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/c-a-ray/dkit/internal/core"
)

// FmtOpts configures how files are reformatted
type FmtOpts struct {
	OutDelim   rune
	OutDir     string
	OutExt     string
	Config     *core.Config
	InPlace    bool
	OutputPath string
}

// RewriteDelimiter reformats one or more files
// If opts.OutDir != "", writes each input to OutDir/basename + OutExt
// Else if OutputPath != "", writes to that path (requires exactly one input)
// Else writes to stdout (requires exactly one input)
func RewriteDelimiter(files []string, opts FmtOpts) error {
	if len(files) == 0 {
		return fmt.Errorf("no files")
	}
	if opts.InPlace {
		return rewriteInPlace(files, opts)
	}
	if opts.OutDir == "" {
		return rewriteOneFile(files[0], opts)
	}
	return rewriteFiles(files, opts)
}

func rewriteInPlace(files []string, opts FmtOpts) error {
	for _, in := range files {
		fi, err := os.Stat(in)
		if err != nil {
			return err
		}

		dir := filepath.Dir(in)
		tmp, err := os.CreateTemp(dir, filepath.Base(in)+".*.dkit")
		if err != nil {
			return err
		}
		tmpPath := tmp.Name()

		if err := writeFile(in, tmp, opts); err != nil {
			tmp.Close()
			os.Remove(tmpPath)
			return err
		}

		_ = tmp.Sync()
		_ = tmp.Chmod(fi.Mode())
		if err := tmp.Close(); err != nil {
			os.Remove(tmpPath)
			return err
		}

		if err := os.Rename(tmpPath, in); err != nil {
			_ = os.Remove(in)
			if err2 := os.Rename(tmpPath, in); err2 != nil {
				_ = os.Remove(tmpPath)
				return err
			}
		}

		if !opts.Config.Quiet {
			fmt.Fprintln(os.Stderr, in)
		}
	}

	return nil
}

func rewriteOneFile(file string, opts FmtOpts) error {
	var out io.Writer = os.Stdout
	if opts.OutputPath != "" {
		f, err := os.Create(opts.OutputPath)
		if err != nil {
			return err
		}
		defer f.Close()
		out = f
	}

	return writeFile(file, out, opts)
}

func rewriteFiles(files []string, opts FmtOpts) error {
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return err
	}

	for _, file := range files {
		base := filepath.Base(file)
		ext := filepath.Ext(base)
		name := strings.TrimSuffix(base, ext)
		outPath := filepath.Join(opts.OutDir, name+opts.OutExt)

		f, err := os.Create(outPath)
		if err != nil {
			return err
		}

		if err := writeFile(file, f, opts); err != nil {
			return err
		}

		if !opts.Config.Quiet {
			fmt.Fprintln(os.Stderr, outPath)
		}
	}

	return nil
}

func writeFile(inPath string, w io.Writer, opts FmtOpts) error {
	rc, err := core.OpenWithEncoding(inPath, opts.Config.Encoding)
	if err != nil {
		return fmt.Errorf("open %s: %w", inPath, err)
	}
	defer rc.Close()

	r := core.NewCSVReader(rc, opts.Config.Delim, opts.Config.LazyQuotes)
	out := csv.NewWriter(w)
	out.Comma = opts.OutDelim

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("%s: %w", inPath, err)
		}

		if err := out.Write(rec); err != nil {
			return fmt.Errorf("write: %w", err)
		}
	}

	out.Flush()
	return out.Error()
}
