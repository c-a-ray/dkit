package ops

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/c-a-ray/dkit/internal/core"
)

// FilesWithOpts configures how to search files for a column/value match
type FilesWithOpts struct {
	Column          string
	Value           string
	CaseInsensitive bool
	Config          *core.Config
}

// FilesWith scans the given CSV files and prints the path of each file that
// contains at least one row where Column equals Value
func FilesWith(files []string, o FilesWithOpts) (int, error) {
	printed := 0
	want := o.Value
	if o.CaseInsensitive {
		want = strings.ToLower(want)
	}
	for _, path := range files {
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
				return printed, fmt.Errorf("--no-header requires numeric index, got %q", o.Column)
			}
		} else {
			hdr, err := cr.Read()
			if err == io.EOF {
				rc.Close()
				continue
			}
			if err != nil {
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

		matched := false
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
			if o.CaseInsensitive {
				v = strings.ToLower(v)
			}
			if v == want {
				fmt.Println(path)
				printed++
				matched = true
				break
			}
		}
		rc.Close()
		_ = matched
	}
	return printed, nil
}
