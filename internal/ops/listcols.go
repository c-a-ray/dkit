package ops

import (
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/c-a-ray/dkit/internal/core"
)

type ListColsOpts struct {
	Config *core.Config
}

// ListColumns prints unique column names from all provided files
func ListColumns(files []string, o ListColsOpts) error {
	if len(files) == 0 {
		return fmt.Errorf("no files")
	}

	cols := map[string]struct{}{}

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
			cols[h] = struct{}{}
		}
		rc.Close()
	}

	out := make([]string, 0, len(cols))
	for c := range cols {
		out = append(out, c)
	}
	sort.Strings(out)

	for _, c := range out {
		fmt.Println(c)
	}

	return nil
}
