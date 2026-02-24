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
	rootCmd.PersistentFlags().BoolP("noHeader", "H", false, "treat first row as data (numeric column indexes)")
	rootCmd.PersistentFlags().BoolP("quiet", "q", false, "suppress per-row output where applicable")
	rootCmd.PersistentFlags().Bool("lazyQuotes", false, "allow bare quotes inside unquoted fields")
	rootCmd.PersistentFlags().Int("skipStart", 0, "number of raw lines to skip at the start of each file")
	rootCmd.PersistentFlags().Int("skipEnd", 0, "number of raw lines to skip at the end of each file")
	rootCmd.PersistentFlags().String("fieldsPerRecord", "", `expected fields per record ("variable" for any, or an integer)`)

	addColCmd(rootCmd, cfg)
	addFilesCmd(rootCmd, cfg)
	addFmtCmd(rootCmd, cfg)
	addCmpCmd(rootCmd, cfg)
	addNPICmd(rootCmd, cfg)

	return rootCmd
}
