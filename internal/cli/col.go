package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/c-a-ray/dkit/internal/core"
	"github.com/c-a-ray/dkit/internal/ops"
	"github.com/spf13/cobra"
)

func addColCmd(parent *cobra.Command, cfg *core.Config) {
	colCmd := &cobra.Command{
		Use:   "col",
		Short: "Column-oriented operations",
	}

	colCmd.AddCommand(newColCmpCmd(cfg))
	colCmd.AddCommand(newColValsCmd(cfg))
	colCmd.AddCommand(newColFirstCmd(cfg))
	colCmd.AddCommand(newColDupKeyCmd(cfg))
	colCmd.AddCommand(newColListCmd(cfg))

	parent.AddCommand(colCmd)
}

func newColCmpCmd(cfg *core.Config) *cobra.Command {
	var ignoreCase, allowEmpty bool
	var toFlag string

	cmd := &cobra.Command{
		Use:   "cmp <A> <B[,C,...]> [files...]",
		Short: "Compare column A against one or more target columns (OR logic)",
		Long: `Compare column A against one or more target columns.

A row is a match if A equals ANY of the target columns (OR logic).
Use comma-separated column names to specify multiple targets.

Examples:
  dkit col cmp ID ID2 *.csv           # A must equal B
  dkit col cmp Name First,Last *.csv  # Name must equal First OR Last`,
		Args: cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			A := args[0]
			targets := splitComma(args[1])

			list, err := resolveFiles(args[2:], cfg)
			if err != nil {
				return err
			}

			format := ops.OutputLines
			var csvPath string
			if toFlag != "" {
				parts := strings.SplitN(toFlag, " ", 2)
				switch parts[0] {
				case "lines":
					format = ops.OutputLines
				case "table":
					format = ops.OutputTable
				case "csv":
					format = ops.OutputCSV
					if len(parts) < 2 || parts[1] == "" {
						return fmt.Errorf("--to csv requires a path argument (e.g., --to 'csv /path/to/output.csv')")
					}
					csvPath = parts[1]
				default:
					return fmt.Errorf("unknown --to format %q (use lines, table, or csv)", parts[0])
				}
			}

			res, err := ops.CompareColumns(list, ops.CompareOpts{
				ColA:       A,
				TargetCols: targets,
				IgnoreCase: ignoreCase,
				AllowEmpty: allowEmpty,
				Quiet:      cfg.Quiet,
				Format:     format,
				CSVPath:    csvPath,
				Config:     cfg,
			})
			if err != nil {
				return err
			}
			if res.Mismatches > 0 {
				os.Exit(2)
			}

			return nil
		},
	}

	cmd.Flags().BoolVar(&ignoreCase, "ignoreCase", false, "case-insensitive comparison")
	cmd.Flags().BoolVar(&allowEmpty, "allowEmpty", false, "compare even if one/both empty")
	cmd.Flags().StringVar(&toFlag, "to", "", "output format: lines (default), table, or 'csv /path/to/file.csv'")

	return cmd
}

func newColValsCmd(cfg *core.Config) *cobra.Command {
	var nullTok string
	var fixed string
	var whenFlags []string

	cmd := &cobra.Command{
		Use:   "vals <uniq|freq> <COL> [files...]",
		Short: "Unique values or value counts for a column",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sub := args[0]
			col := args[1]

			if sub != "uniq" && sub != "freq" {
				return fmt.Errorf("first arg must be uniq|freq")
			}

			list, err := resolveFiles(args[2:], cfg)
			if err != nil {
				return err
			}

			filter, err := ops.ParseWhenFlags(whenFlags)
			if err != nil {
				return fmt.Errorf("invalid --when: %w", err)
			}

			opt := ops.ValsOpts{
				Column:    col,
				Mode:      ops.ValsUniq,
				NullToken: nullTok,
				Filter:    filter,
				Config:    cfg,
			}

			if sub == "freq" {
				opt.Mode = ops.ValsFreq
			}

			if fixed != "" {
				var s, e int
				if _, err := fmt.Sscanf(fixed, "%d:%d", &s, &e); err != nil {
					return fmt.Errorf("--fixedWidth expects START:END, got %q", fixed)
				}
				opt.FixedStart, opt.FixedEnd = s, e
			}

			return ops.ColumnValues(list, opt)
		},
	}

	cmd.Flags().StringVar(&nullTok, "nullToken", "<EMPTY>", "token to print for empty cells (freq only)")
	cmd.Flags().StringVar(&fixed, "fixedWidth", "", "use fixed-width extraction START:END (0-based, exclusive end)")
	cmd.Flags().StringArrayVarP(&whenFlags, "when", "w", nil, "filter rows by condition (repeatable, ANDed; use | for OR)")

	return cmd
}

func newColFirstCmd(cfg *core.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "first <COL> [files...]",
		Short: "Print first non-empty value per file for a column",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			col := args[0]

			list, err := resolveFiles(args[1:], cfg)
			if err != nil {
				return err
			}

			n, err := ops.FirstNonEmpty(list, ops.FirstOpts{
				Column: col,
				Config: cfg,
			})
			if err != nil {
				return err
			}
			if n == 0 {
				os.Exit(2)
			}

			return nil
		},
	}

	return cmd
}

func newColDupKeyCmd(cfg *core.Config) *cobra.Command {
	var by string
	var ignoreCase bool
	var requireAll bool
	var nullTok string

	cmd := &cobra.Command{
		Use:   "dupkey <KEY> [files...]",
		Short: "Report keys that map to multiple distinct tuples (e.g., First+Last)",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			key := args[0]
			if by == "" {
				return fmt.Errorf("--by is required (comma-separated columns)")
			}
			list, err := resolveFiles(args[1:], cfg)
			if err != nil {
				return err
			}
			opts := ops.DupKeyOpts{
				Key:        key,
				ByColumns:  splitComma(by),
				IgnoreCase: ignoreCase,
				RequireAll: requireAll,
				NullToken:  nullTok,
				Quiet:      cfg.Quiet,
				Config:     cfg,
			}
			res, err := ops.DupKey(list, opts)
			if err != nil {
				return err
			}
			if res.ConflictingKeys > 0 {
				os.Exit(2)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&by, "by", "", "comma-separated list of columns forming the person tuple (e.g., \"Patient First Name,Patient Last Name\")")
	cmd.Flags().BoolVar(&ignoreCase, "ignoreCase", false, "case-insensitive comparisons")
	cmd.Flags().BoolVar(&requireAll, "requireAll", false, "skip rows where any BY field is empty")
	cmd.Flags().StringVar(&nullTok, "nullToken", "<EMPTY>", "token to substitute for empty BY fields (ignored if --requireAll)")

	return cmd
}

func newColListCmd(cfg *core.Config) *cobra.Command {
	var sorted, oneline bool
	var onelineDelim string

	cmd := &cobra.Command{
		Use:   "list [files...]",
		Short: "List unique column names across files",
		Args:  cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			list, err := resolveFiles(args, cfg)
			if err != nil {
				return err
			}

			delim, err := core.ParseDelim(onelineDelim)
			if err != nil {
				return err
			}

			return ops.ListColumns(list, ops.ListColsOpts{
				Sorted:       sorted,
				OneLine:      oneline,
				OneLineDelim: string(delim),
				Config:       cfg,
			})
		},
	}

	cmd.Flags().BoolVar(&sorted, "sorted", false, "sort columns alphabetically")
	cmd.Flags().BoolVar(&oneline, "oneline", false, "print all columns on one line")
	cmd.Flags().StringVarP(&onelineDelim, "outdelim", "o", "comma", "output delimiter for --oneline (tab, comma, pipe, space, or single char)")

	return cmd
}

func splitComma(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
