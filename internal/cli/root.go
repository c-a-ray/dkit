package cli

import (
	"fmt"

	"github.com/c-a-ray/dkit/internal/core"
	"github.com/spf13/cobra"
)

// NewRootCmd constructs the root command for dkit
func NewRootCmd(cfg *core.Config) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "dkit",
		Short: "A toolkit for exploring tabular data",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Load active config from global state
			cfgPath, err := core.GetActiveConfig()
			if err != nil {
				return err
			}
			if cfgPath != "" {
				fc, err := core.LoadFileConfig(cfgPath)
				if err != nil {
					return err
				}
				if err := fc.ApplyTo(cfg); err != nil {
					return err
				}
			}

			// CLI flags override config file values (only explicitly-set flags)
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
	addConfigCmd(rootCmd)

	return rootCmd
}

// resolveFiles returns the expanded file list, falling back to cfg.Files
// if no patterns are provided from command-line args.
func resolveFiles(patterns []string, cfg *core.Config) ([]string, error) {
	if len(patterns) == 0 {
		patterns = cfg.Files
	}
	if len(patterns) == 0 {
		return nil, fmt.Errorf("no files specified (provide file args or set a config file)")
	}
	return core.ExpandFiles(patterns)
}
