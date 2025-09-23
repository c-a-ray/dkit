package cli

import (
	"github.com/c-a-ray/dkit/internal/core"
	"github.com/spf13/cobra"
)

// NewRootCmd constructs the root command for dkit
func NewRootCmd(cfg *core.Config) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "dkit",
		Short: "A toolkit for exploring tabular data",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return cfg.FromFlags(cmd.Flags())
		},
	}

	rootCmd.PersistentFlags().StringP("delim", "d", ",", "field delimiter (single char)")
	rootCmd.PersistentFlags().StringP("encoding", "e", "utf-8-sig", "input encoding")
	rootCmd.PersistentFlags().BoolP("no-header", "H", false, "treat first row as data (numeric column indexes)")
	rootCmd.PersistentFlags().BoolP("quiet", "q", false, "suppress per-row output where applicable")
	rootCmd.PersistentFlags().Bool("lazy-quotes", false, "allow bare quotes inside unquoted fields")

	addColCmd(rootCmd, cfg)
	addFilesCmd(rootCmd, cfg)
	addFmtCmd(rootCmd, cfg)

	return rootCmd
}
