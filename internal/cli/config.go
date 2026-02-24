package cli

import (
	"fmt"
	"os"

	"github.com/c-a-ray/dkit/internal/core"
	"github.com/spf13/cobra"
)

func addConfigCmd(parent *cobra.Command) {
	configCmd := &cobra.Command{
		Use:   "config",
		Short: "Manage dkit configuration",
		// Override PersistentPreRunE to skip config loading for config commands
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}

	configCmd.AddCommand(newConfigSetCmd())
	configCmd.AddCommand(newConfigResetCmd())
	configCmd.AddCommand(newConfigShowCmd())

	parent.AddCommand(configCmd)
}

func newConfigSetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "set <path-to-config.yaml>",
		Short: "Set the config file for the current directory",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			wd, err := os.Getwd()
			if err != nil {
				return err
			}
			cfgPath := args[0]
			if _, err := core.LoadFileConfig(cfgPath); err != nil {
				return fmt.Errorf("cannot use config: %w", err)
			}
			if err := core.WriteDotfile(wd, cfgPath); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "Config set: %s\n", cfgPath)
			return nil
		},
	}
}

func newConfigResetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reset",
		Short: "Remove the .dkit config pointer from the current directory",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			wd, err := os.Getwd()
			if err != nil {
				return err
			}
			if err := core.RemoveDotfile(wd); err != nil {
				return err
			}
			fmt.Fprintln(os.Stderr, "Config reset")
			return nil
		},
	}
}

func newConfigShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show",
		Short: "Show the active config file and its contents",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			wd, err := os.Getwd()
			if err != nil {
				return err
			}
			cfgPath, err := core.LoadDotfile(wd)
			if err != nil {
				return err
			}
			if cfgPath == "" {
				fmt.Println("No config file set (no .dkit in current directory)")
				return nil
			}
			fc, err := core.LoadFileConfig(cfgPath)
			if err != nil {
				return err
			}
			fmt.Printf("Config file: %s\n\n", cfgPath)
			printFileConfig(fc)
			return nil
		},
	}
}

func printFileConfig(fc *core.FileConfig) {
	if len(fc.Files) > 0 {
		fmt.Println("files:")
		for _, f := range fc.Files {
			fmt.Printf("  - %s\n", f)
		}
	}
	if fc.Delimiter != "" {
		fmt.Printf("delimiter: %s\n", fc.Delimiter)
	}
	if fc.Encoding != "" {
		fmt.Printf("encoding: %s\n", fc.Encoding)
	}
	if fc.NoHeader != nil {
		fmt.Printf("noHeader: %v\n", *fc.NoHeader)
	}
	if fc.Quiet != nil {
		fmt.Printf("quiet: %v\n", *fc.Quiet)
	}
	if fc.LazyQuotes != nil {
		fmt.Printf("lazyQuotes: %v\n", *fc.LazyQuotes)
	}
	if fc.SkipStart != nil {
		fmt.Printf("skipStart: %d\n", *fc.SkipStart)
	}
	if fc.SkipEnd != nil {
		fmt.Printf("skipEnd: %d\n", *fc.SkipEnd)
	}
	if fc.FieldsPerRecord != "" {
		fmt.Printf("fieldsPerRecord: %s\n", fc.FieldsPerRecord)
	}
	if len(fc.FixedColumns) > 0 {
		fmt.Println("fixedColumns:")
		for _, col := range fc.FixedColumns {
			fmt.Printf("  - name: %s, start: %d, end: %d\n", col.Name, col.Start, col.End)
		}
	}
}
