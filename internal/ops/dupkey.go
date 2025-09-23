package ops

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/c-a-ray/dkit/internal/core"
)

type DupKeyOpts struct {
	Key        string
	ByColumns  []string
	IgnoreCase bool
	RequireAll bool
	NullToken  string
	Quiet      bool
	Config     *core.Config
}

type DupKeyResult struct {
	FilesScanned    int
	RowsSeen        int
	ConflictingKeys int
}

// DupKey finds keys that map to more than one distinct tuple of BY fields.
func DupKey(files []string, o DupKeyOpts) (DupKeyResult, error) {
	res := DupKeyResult{}
	if len(files) == 0 {
		return res, fmt.Errorf("no files")
	}

	keyToTuples := map[string]map[string]int{}

	for _, path := range files {
		rc, err := core.OpenWithEncoding(path, o.Config.Encoding)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] cannot read %s: %v\n", path, err)
			continue
		}
		cr := core.NewCSVReader(rc, o.Config.Delim, o.Config.LazyQuotes)

		var idxKey int
		var idxBy []int

		if o.Config.NoHeader {
			ki, err := parseIndex(o.Key)
			if err != nil {
				rc.Close()
				return res, fmt.Errorf("--no-header: key must be index: %w", err)
			}

			idxKey = ki
			idxBy = make([]int, len(o.ByColumns))
			for i, s := range o.ByColumns {
				vi, err := parseIndex(s)
				if err != nil {
					rc.Close()
					return res, fmt.Errorf("--no-header: by[%d] must be index: %w", i, err)
				}
				idxBy[i] = vi
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
			ik, err := resolveHeaderIndex(hdr, o.Key)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[WARN] %s: %v\n", path, err)
				rc.Close()
				continue
			}
			idxKey = ik
			idxBy = make([]int, len(o.ByColumns))
			for i, name := range o.ByColumns {
				j, err := resolveHeaderIndex(hdr, name)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[WARN] %s: %v\n", path, err)
					rc.Close()
					idxBy = nil
					break
				}
				idxBy[i] = j
			}
			if idxBy == nil {
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
			res.RowsSeen++

			if idxKey >= len(rec) {
				continue
			}
			key := strings.TrimSpace(rec[idxKey])
			if key == "" {
				continue
			}

			vals := make([]string, len(idxBy))
			missing := false
			for i, j := range idxBy {
				if j >= len(rec) {
					missing = true
					break
				}
				v := strings.TrimSpace(rec[j])
				if o.RequireAll && v == "" {
					missing = true
					break
				}
				if v == "" && !o.RequireAll && o.NullToken != "" {
					v = o.NullToken
				}
				vals[i] = v
			}
			if missing {
				continue
			}

			if o.IgnoreCase {
				key = strings.ToLower(key)
				for i := range vals {
					vals[i] = strings.ToLower(vals[i])
				}
			}

			tuple := strings.Join(vals, "||") // safe internal separator

			if keyToTuples[key] == nil {
				keyToTuples[key] = map[string]int{}
			}
			keyToTuples[key][tuple]++
		}

		rc.Close()
		res.FilesScanned++
	}

	for k, m := range keyToTuples {
		if len(m) <= 1 {
			continue
		}
		res.ConflictingKeys++
		if !o.Quiet {
			fmt.Printf("KEY: %s  (%d distinct BY-tuples)\n", k, len(m))
			// stable-ish order: by count desc, then tuple asc
			type pair struct {
				t string
				c int
			}
			out := make([]pair, 0, len(m))
			for t, c := range m {
				out = append(out, pair{t, c})
			}
			sort.Slice(out, func(i, j int) bool {
				if out[i].c == out[j].c {
					return out[i].t < out[j].t
				}
				return out[i].c > out[j].c
			})
			for _, p := range out {
				parts := strings.Split(p.t, "||")
				fmt.Printf("  - (%s)  %d\n", joinKV(o.ByColumns, parts), p.c)
			}
			fmt.Println()
		}
	}

	fmt.Fprintf(os.Stderr, "\nScanned %d files, %d rows. Conflicting keys: %d\n",
		res.FilesScanned, res.RowsSeen, res.ConflictingKeys)

	return res, nil
}
