package core

import (
	"errors"
	"path/filepath"
)

// ExpandFiles expands glob patterns into a deduplicated list of file paths
func ExpandFiles(patterns []string) ([]string, error) {
	if len(patterns) == 0 {
		return nil, errors.New("no files provided")
	}

	seen := make(map[string]struct{})
	order := make([]string, 0, len(patterns))

	for _, pat := range patterns {
		matches, err := filepath.Glob(pat)
		if err != nil {
			return nil, err
		}

		for _, m := range matches {
			if _, ok := seen[m]; !ok {
				seen[m] = struct{}{}
				order = append(order, m)
			}
		}
	}

	return order, nil
}
