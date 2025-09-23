package ops

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/c-a-ray/dkit/internal/core"
)

// FirstOpts configures how the first non-empty value is searched
type FirstOpts struct {
	Column string
	Config *core.Config
}

// FirstNonEmpty scans the given files and prints the first non-empty value
// found in the specified column of each file
func FirstNonEmpty(files []string, o FirstOpts) (int, error) {
	printed := 0
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
		found := false
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
			if v != "" {
				fmt.Printf("%s: %s\n", path, v)
				printed++
				found = true
				break
			}
		}
		rc.Close()
		_ = found
	}
	return printed, nil
}
