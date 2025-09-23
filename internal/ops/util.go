package ops

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func parseIndex(s string) (int, error) {
	i, err := strconv.Atoi(s)
	if err != nil || i < 0 {
		return 0, fmt.Errorf("invalid index %q", s)
	}
	return i, nil
}

func resolveHeaderIndex(header []string, name string) (int, error) {
	for i, h := range header {
		if h == name {
			return i, nil
		}
	}
	return 0, fmt.Errorf("header %q not found", name)
}

func filepathBase(p string) string {
	i := strings.LastIndexByte(p, os.PathSeparator)
	if i < 0 {
		return p
	}
	return p[i+1:]
}

func joinKV(names, vals []string) string {
	n := min(len(names), len(vals))
	parts := make([]string, n)
	for i := range n {
		parts[i] = fmt.Sprintf("%s=%q", names[i], vals[i])
	}
	return strings.Join(parts, ", ")
}
