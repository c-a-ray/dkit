package cli

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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

	configCmd.AddCommand(newConfigCreateCmd())
	configCmd.AddCommand(newConfigRegisterCmd())
	configCmd.AddCommand(newConfigUnregisterCmd())
	configCmd.AddCommand(newConfigSetCmd())
	configCmd.AddCommand(newConfigGetCmd())
	configCmd.AddCommand(newConfigResetCmd())
	configCmd.AddCommand(newConfigListCmd())

	parent.AddCommand(configCmd)
}

func newConfigCreateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "create",
		Short: "Interactively create a new dkit config file",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			scanner := bufio.NewScanner(os.Stdin)

			fmt.Print("Project name: ")
			if !scanner.Scan() {
				return fmt.Errorf("cancelled")
			}
			name := strings.TrimSpace(scanner.Text())
			if name == "" {
				return fmt.Errorf("project name cannot be empty")
			}

			fmt.Print("Directory to create config file in: ")
			if !scanner.Scan() {
				return fmt.Errorf("cancelled")
			}
			dir := strings.TrimSpace(scanner.Text())
			if dir == "" {
				return fmt.Errorf("directory cannot be empty")
			}

			absDir, err := filepath.Abs(dir)
			if err != nil {
				return err
			}
			info, err := os.Stat(absDir)
			if err != nil {
				return fmt.Errorf("directory %s: %w", absDir, err)
			}
			if !info.IsDir() {
				return fmt.Errorf("%s is not a directory", absDir)
			}

			cfgPath := filepath.Join(absDir, name+".yaml")
			if _, err := os.Stat(cfgPath); err == nil {
				return fmt.Errorf("file already exists: %s", cfgPath)
			}

			fmt.Print("Set as active config now? [y/N]: ")
			activate := false
			if scanner.Scan() {
				ans := strings.ToLower(strings.TrimSpace(scanner.Text()))
				activate = ans == "y" || ans == "yes"
			}

			fc := &core.FileConfig{Name: name}
			if err := core.SaveFileConfig(cfgPath, fc); err != nil {
				return err
			}

			if _, err := core.RegisterConfig(cfgPath); err != nil {
				return err
			}

			if activate {
				if err := core.SetActiveConfig(cfgPath); err != nil {
					return err
				}
				fmt.Fprintf(os.Stderr, "Created and activated: %s\n", cfgPath)
			} else {
				fmt.Fprintf(os.Stderr, "Created: %s\n", cfgPath)
			}
			fmt.Fprintf(os.Stderr, "\nTo set options, edit the file directly or use:\n")
			fmt.Fprintf(os.Stderr, "  dkit config set <key> <value>\n")
			if !activate {
				fmt.Fprintf(os.Stderr, "\nTo activate this config:\n")
				fmt.Fprintf(os.Stderr, "  dkit config set %s\n", name)
			}
			return nil
		},
	}
}

func newConfigRegisterCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "register <path-to-config.yaml>",
		Short: "Register an existing config file by its name field",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name, err := core.RegisterConfig(args[0])
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "Registered %q from %s\n", name, args[0])
			return nil
		},
	}
}

func newConfigUnregisterCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "unregister <name>",
		Short: "Remove a config from the registry",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := core.UnregisterConfig(args[0]); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "Unregistered %q\n", args[0])
			return nil
		},
	}
}

func newConfigSetCmd() *cobra.Command {
	var startFlag, endFlag int

	cmd := &cobra.Command{
		Use:   "set <name-or-path | key value | fixedColumns add <name> | files add <pattern>...>",
		Short: "Set active config, or modify a config option",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// 1 arg: set active config by name or path
			if len(args) == 1 {
				return setActiveByNameOrPath(args[0])
			}

			key := args[0]

			// fixedColumns add <name> --start N --end N
			if key == "fixedColumns" && len(args) >= 3 && args[1] == "add" {
				colName := args[2]
				if !cmd.Flags().Changed("start") || !cmd.Flags().Changed("end") {
					return fmt.Errorf("--start and --end are required for fixedColumns add")
				}
				return addFixedColumn(colName, startFlag, endFlag)
			}

			// files add <pattern>...
			if key == "files" && len(args) >= 3 && args[1] == "add" {
				return addFiles(args[2:])
			}

			// <key> <value>
			if len(args) != 2 {
				return fmt.Errorf("usage: dkit config set <key> <value>")
			}
			return setConfigKey(key, args[1])
		},
	}

	cmd.Flags().IntVar(&startFlag, "start", 0, "start position for fixedColumns add")
	cmd.Flags().IntVar(&endFlag, "end", 0, "end position for fixedColumns add")

	return cmd
}

func newConfigGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "get [options]",
		Short: "Show active config info",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 && args[0] == "options" {
				return showActiveConfigOptions()
			}
			if len(args) == 1 {
				return fmt.Errorf("unknown argument %q (did you mean `dkit config get options`?)", args[0])
			}
			path, err := core.GetActiveConfig()
			if err != nil {
				return err
			}
			if path == "" {
				fmt.Println("No active config")
				return nil
			}
			fc, err := core.LoadFileConfig(path)
			if err != nil {
				return err
			}
			if fc.Name != "" {
				fmt.Printf("name: %s\n", fc.Name)
			}
			fmt.Printf("path: %s\n", path)
			return nil
		},
	}
}

func newConfigResetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reset",
		Short: "Clear the active config pointer (does not delete or unregister)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := core.ResetActiveConfig(); err != nil {
				return err
			}
			fmt.Fprintln(os.Stderr, "Active config cleared")
			return nil
		},
	}
}

func newConfigListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all registered configs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			gs, err := core.ListConfigs()
			if err != nil {
				return err
			}
			if len(gs.Configs) == 0 {
				fmt.Println("No configs registered")
				return nil
			}
			for name, path := range gs.Configs {
				marker := "  "
				if path == gs.Active {
					marker = "* "
				}
				fmt.Printf("%s%s: %s\n", marker, name, path)
			}
			return nil
		},
	}
}

// setActiveByNameOrPath resolves a name or path and sets it as active.
func setActiveByNameOrPath(arg string) error {
	// Try as registered name first
	gs, err := core.LoadGlobalState()
	if err != nil {
		return err
	}
	if _, ok := gs.Configs[arg]; ok {
		if err := core.SetActiveConfigByName(arg); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "Active config set to %q (%s)\n", arg, gs.Configs[arg])
		return nil
	}

	// Try as file path
	abs, err := filepath.Abs(arg)
	if err != nil {
		return err
	}
	if _, err := os.Stat(abs); err != nil {
		return fmt.Errorf("%q is not a registered config name and not a valid file path", arg)
	}

	if err := core.SetActiveConfig(abs); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Active config set to: %s\n", abs)
	return nil
}

// loadActiveForEdit returns the active config path and parsed FileConfig,
// erroring if no config is active.
func loadActiveForEdit() (string, *core.FileConfig, error) {
	path, err := core.GetActiveConfig()
	if err != nil {
		return "", nil, err
	}
	if path == "" {
		return "", nil, fmt.Errorf("no config is currently active. Create one with `dkit config create`")
	}
	fc, err := core.LoadFileConfig(path)
	if err != nil {
		return "", nil, err
	}
	return path, fc, nil
}

var knownKeys = map[string]bool{
	"delimiter":       true,
	"encoding":        true,
	"noHeader":        true,
	"quiet":           true,
	"lazyQuotes":      true,
	"skipStart":       true,
	"skipEnd":         true,
	"fieldsPerRecord": true,
}

func setConfigKey(key, value string) error {
	if !knownKeys[key] {
		return fmt.Errorf("unknown config key %q (valid keys: delimiter, encoding, noHeader, quiet, lazyQuotes, skipStart, skipEnd, fieldsPerRecord)", key)
	}

	path, fc, err := loadActiveForEdit()
	if err != nil {
		return err
	}

	switch key {
	case "delimiter":
		if _, err := core.ParseDelim(value); err != nil {
			return err
		}
		fc.Delimiter = value
	case "encoding":
		fc.Encoding = value
	case "noHeader":
		b, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("noHeader must be true or false, got %q", value)
		}
		fc.NoHeader = &b
	case "quiet":
		b, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("quiet must be true or false, got %q", value)
		}
		fc.Quiet = &b
	case "lazyQuotes":
		b, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("lazyQuotes must be true or false, got %q", value)
		}
		fc.LazyQuotes = &b
	case "skipStart":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("skipStart must be an integer, got %q", value)
		}
		fc.SkipStart = &n
	case "skipEnd":
		n, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("skipEnd must be an integer, got %q", value)
		}
		fc.SkipEnd = &n
	case "fieldsPerRecord":
		fc.FieldsPerRecord = value
	}

	if err := core.SaveFileConfig(path, fc); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Set %s = %s in %s\n", key, value, path)
	return nil
}

func addFixedColumn(name string, start, end int) error {
	path, fc, err := loadActiveForEdit()
	if err != nil {
		return err
	}
	fc.FixedColumns = append(fc.FixedColumns, core.FixedColumnDef{
		Name:  name,
		Start: start,
		End:   end,
	})
	if err := core.SaveFileConfig(path, fc); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Added fixedColumn %s (%d:%d) in %s\n", name, start, end, path)
	return nil
}

func addFiles(patterns []string) error {
	path, fc, err := loadActiveForEdit()
	if err != nil {
		return err
	}
	fc.Files = append(fc.Files, patterns...)
	if err := core.SaveFileConfig(path, fc); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Added %d file pattern(s) to %s\n", len(patterns), path)
	return nil
}

func showActiveConfigOptions() error {
	path, err := core.GetActiveConfig()
	if err != nil {
		return err
	}
	if path == "" {
		fmt.Println("No active config")
		return nil
	}
	fc, err := core.LoadFileConfig(path)
	if err != nil {
		return err
	}
	fmt.Printf("Config file: %s\n\n", path)
	printFileConfig(fc)
	return nil
}

func printFileConfig(fc *core.FileConfig) {
	if fc.Name != "" {
		fmt.Printf("name: %s\n", fc.Name)
	}
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
