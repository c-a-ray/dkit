package ops

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/c-a-ray/dkit/internal/core"
)

// ValsMode selects how column values are reported
type ValsMode int

const (
	// ValsUniq prints unique values only
	ValsUniq ValsMode = iota
	// ValsFreq prints values with their frequencies
	ValsFreq
)

// ValueOpts configures how column values are collected and printed
// Column may be a header name or an index string when --no-header is set
// FixedStart/FixedEnd enable fixed-width extraction (1-based, inclusive)
type ValsOpts struct {
	Column     string
	Mode       ValsMode
	NullToken  string
	FixedStart int
	FixedEnd   int
	Config     *core.Config
}

// ColumnValues prints values from the specified column across files,
// either as unique values or as value frequencies, per ValsOpts
func ColumnValues(files []string, o ValsOpts) error {
	if len(files) == 0 {
		return errors.New("no files")
	}
	uniq := map[string]struct{}{}
	freq := map[string]int{}

	for _, path := range files {
		if o.FixedStart > 0 && o.FixedEnd >= o.FixedStart {
			if err := scanFixed(path, o, uniq, freq); err != nil {
				fmt.Fprintf(os.Stderr, "[WARN] %s: %v\n", path, err)
			}
			continue
		}

		rc, err := core.OpenWithEncoding(path, o.Config.Encoding)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] cannot read %s: %v\n", path, err)
			continue
		}
		cr := core.NewCSVReader(rc, o.Config.Delim, o.Config.LazyQuotes)

		var idx int
		if o.Config.NoHeader {
			idx, err = strconv.Atoi(o.Column)
			if err != nil || idx < 0 {
				rc.Close()
				return fmt.Errorf("--no-header requires numeric column index, got %q", o.Column)
			}
		} else {
			hdr, err := cr.Read()
			if err == io.EOF {
				fmt.Fprintf(os.Stderr, "[WARN] %s is empty\n", path)
				rc.Close()
				continue
			} else if err != nil {
				fmt.Fprintf(os.Stderr, "[WARN] %s: %v\n", path, err)
				rc.Close()
				continue
			}
			idx = -1
			for i, h := range hdr {
				if h == o.Column {
					idx = i
					break
				}
			}
			if idx < 0 {
				fmt.Fprintf(os.Stderr, "[WARN] %s: header %q not found\n", path, o.Column)
				rc.Close()
				continue
			}
		}

		for {
			rec, err := cr.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "[WARN] %s: %v\n", path, err)
				break
			}
			if idx >= len(rec) {
				continue
			}
			v := strings.TrimSpace(rec[idx])
			if v == "" && o.NullToken != "" {
				v = o.NullToken
			}
			if o.Mode == ValsUniq {
				uniq[v] = struct{}{}
			} else {
				freq[v]++
			}
		}
		rc.Close()
	}

	switch o.Mode {
	case ValsUniq:
		out := make([]string, 0, len(uniq))
		for v := range uniq {
			out = append(out, v)
		}
		sort.Strings(out)
		for _, v := range out {
			fmt.Println(v)
		}
	case ValsFreq:
		type pair struct {
			v string
			c int
		}
		out := make([]pair, 0, len(freq))
		for v, c := range freq {
			out = append(out, pair{v, c})
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].c == out[j].c {
				return out[i].v < out[j].v
			}
			return out[i].c > out[j].c
		})
		for _, p := range out {
			fmt.Printf("%-30s %d\n", p.v, p.c)
		}
	}
	return nil
}

func scanFixed(path string, o ValsOpts, uniq map[string]struct{}, freq map[string]int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	start := o.FixedStart - 1
	width := o.FixedEnd - o.FixedStart + 1
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if start >= len(line) {
			continue
		}

		end := min(start+width, len(line))
		v := strings.TrimSpace(line[start:end])
		if v == "" && o.NullToken != "" {
			v = o.NullToken
		}

		if o.Mode == ValsUniq {
			uniq[v] = struct{}{}
		} else {
			freq[v]++
		}
	}
	return s.Err()
}
