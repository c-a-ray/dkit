package cli

import (
	"fmt"

	"github.com/c-a-ray/dkit/internal/core"
	"github.com/c-a-ray/dkit/internal/ops"
	"github.com/spf13/cobra"
)

func addFmtCmd(root *cobra.Command, cfg *core.Config) {
	var inDelimStr string
	var outDelimStr string
	var outPath string
	var outDir string
	var outExt string
	var inPlace bool

	cmd := &cobra.Command{
		Use:   "fmt [flags] <files...>",
		Short: "Rewrite files with different formatting",
		Args:  cobra.MinimumNArgs(1),
		Example: `
# TSV -> PSV, write to stdout
dkit fmt --in-delim '\t' --out-delim '|' input.tsv > output.psv

# TSV -> PSV, write to file
dkit fmt --in-delim tab --out-delim pipe -o output.psv input.tsv

# CSV -> TSV for many files
dkit fmt --in-delim ',' --out-delim '\t' --outdir out *.csv

# CSV -> TSV, in place over many files
dkit fmt --in-delim comma --out-delim tab --inplace *.csv`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if inDelimStr == "" || outDelimStr == "" {
				return fmt.Errorf("--in-delim and --out-delim are required")
			}
			inDelim, err := core.ParseDelim(inDelimStr)
			if err != nil {
				return fmt.Errorf("--in-delim: %w", err)
			}
			outDelim, err := core.ParseDelim(outDelimStr)
			if err != nil {
				return fmt.Errorf("--out-delim: %w", err)
			}

			if err = validate(args, outDir, outPath, inPlace); err != nil {
				return err
			}

			if outDir != "" && outExt == "" {
				if outDelim == ',' {
					outExt = ".csv"
				} else {
					outExt = ".txt"
				}
			}

			cfg.Delim = inDelim
			opts := ops.FmtOpts{
				OutDelim:   outDelim,
				OutDir:     outDir,
				OutExt:     outExt,
				OutputPath: outPath,
				InPlace:    inPlace,
				Config:     cfg,
			}
			return ops.RewriteDelimiter(args, opts)
		},
	}

	cmd.Flags().StringVar(&inDelimStr, "in-delim", "", "input field delimiter (single char or 'tab')")
	cmd.Flags().StringVar(&outDelimStr, "out-delim", "", "output field delimiter (single char or 'tab')")
	cmd.Flags().StringVarP(&outPath, "out", "o", "", "write to a single output file (requires exactly one input)")
	cmd.Flags().StringVar(&outDir, "outdir", "", "write each input to this directory (one output per input)")
	cmd.Flags().StringVar(&outExt, "ext", "", "output extension used with --outdir")
	cmd.Flags().BoolVarP(&inPlace, "inplace", "i", false, "rewrite the input file(s) in place")

	_ = cmd.MarkFlagRequired("in-delim")
	_ = cmd.MarkFlagRequired("out-delim")

	root.AddCommand(cmd)
}

func validate(files []string, outDir, outPath string, inPlace bool) error {
	if len(files) == 0 {
		return fmt.Errorf("no files")
	}
	if outDir != "" && outPath != "" {
		return fmt.Errorf("--out and --outdir are mutually exclusive")
	}
	if inPlace && (outDir != "" || outPath != "") {
		return fmt.Errorf("--inplace cannot be used with --out or --outdir")
	}
	if !inPlace && outDir == "" && len(files) > 1 && outPath == "" {
		return fmt.Errorf("multiple inputs require --outdir or --inplace")
	}
	return nil
}
