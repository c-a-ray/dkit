package cli

import (
	"os"

	"github.com/c-a-ray/dkit/internal/core"
	"github.com/c-a-ray/dkit/internal/ops"
	"github.com/spf13/cobra"
)

func addCmpCmd(parent *cobra.Command, cfg *core.Config) {
	cmpCmd := &cobra.Command{
		Use:   "cmp",
		Short: "Compare files, directories, and archives",
	}

	cmpCmd.AddCommand(newCmpZipsCmd(cfg))

	parent.AddCommand(cmpCmd)
}

func newCmpZipsCmd(cfg *core.Config) *cobra.Command {
	var summaryOnly bool
	var ignoreMissing bool

	cmd := &cobra.Command{
		Use:   "zips <file_a.zip> <file_b.zip>",
		Short: "Compare two ZIP archives and their contents",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			zipA := args[0]
			zipB := args[1]

			opts := ops.ZipCmpOpts{
				ZipA:          zipA,
				ZipB:          zipB,
				Quiet:         cfg.Quiet,
				SummaryOnly:   summaryOnly,
				IgnoreMissing: ignoreMissing,
				Config:        cfg,
			}

			res, err := ops.CompareZips(opts)
			if err != nil {
				return err
			}

			// Exit with code 2 if there are differences
			if res.Different > 0 || (!ignoreMissing && (res.OnlyInA > 0 || res.OnlyInB > 0)) {
				os.Exit(2)
			}

			return nil
		},
	}

	cmd.Flags().BoolVarP(&summaryOnly, "summary-only", "s", false, "show only summary, skip detailed diffs")
	cmd.Flags().BoolVar(&ignoreMissing, "ignore-missing", false, "don't error if files are missing from one archive")

	return cmd
}
