package ops

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/c-a-ray/dkit/internal/core"
)

type ListColsOpts struct {
	Sorted       bool
	OneLine      bool
	OneLineDelim string
	Config       *core.Config
}

// ListColumns prints unique column names from all provided files
func ListColumns(files []string, o ListColsOpts) error {
	if len(files) == 0 {
		return fmt.Errorf("no files")
	}

	seen := map[string]struct{}{}
	var out []string

	for _, path := range files {
		rc, err := core.OpenWithEncoding(path, o.Config.Encoding)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] cannot read %s: %v\n", path, err)
			continue
		}
		cr := core.NewCSVReader(rc, o.Config.Delim, o.Config.LazyQuotes)

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

		for _, h := range hdr {
			if _, ok := seen[h]; !ok {
				seen[h] = struct{}{}
				out = append(out, h)
			}
		}
		rc.Close()
	}

	if o.Sorted {
		sort.Strings(out)
	}

	if o.OneLine {
		fmt.Println(strings.Join(out, o.OneLineDelim))
	} else {
		for _, c := range out {
			fmt.Println(c)
		}
	}

	return nil
}
