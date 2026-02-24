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
		Args:  cobra.MinimumNArgs(0),
		Example: `
# TSV -> PSV, write to stdout
dkit fmt --inDelim '\t' --outDelim '|' input.tsv > output.psv

# TSV -> PSV, write to file
dkit fmt --inDelim tab --outDelim pipe -o output.psv input.tsv

# CSV -> TSV for many files
dkit fmt --inDelim ',' --outDelim '\t' --outdir out *.csv

# CSV -> TSV, in place over many files
dkit fmt --inDelim comma --outDelim tab --inplace *.csv`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if inDelimStr == "" || outDelimStr == "" {
				return fmt.Errorf("--inDelim and --outDelim are required")
			}
			inDelim, err := core.ParseDelim(inDelimStr)
			if err != nil {
				return fmt.Errorf("--inDelim: %w", err)
			}
			outDelim, err := core.ParseDelim(outDelimStr)
			if err != nil {
				return fmt.Errorf("--outDelim: %w", err)
			}

			files, err := resolveFiles(args, cfg)
			if err != nil {
				return err
			}

			if err = validate(files, outDir, outPath, inPlace); err != nil {
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
			return ops.RewriteDelimiter(files, opts)
		},
	}

	cmd.Flags().StringVar(&inDelimStr, "inDelim", "", "input field delimiter (single char or 'tab')")
	cmd.Flags().StringVar(&outDelimStr, "outDelim", "", "output field delimiter (single char or 'tab')")
	cmd.Flags().StringVarP(&outPath, "out", "o", "", "write to a single output file (requires exactly one input)")
	cmd.Flags().StringVar(&outDir, "outdir", "", "write each input to this directory (one output per input)")
	cmd.Flags().StringVar(&outExt, "ext", "", "output extension used with --outdir")
	cmd.Flags().BoolVarP(&inPlace, "inplace", "i", false, "rewrite the input file(s) in place")

	_ = cmd.MarkFlagRequired("inDelim")
	_ = cmd.MarkFlagRequired("outDelim")

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
